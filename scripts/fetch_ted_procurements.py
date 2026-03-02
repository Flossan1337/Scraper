"""
Fetch EU procurement data from TED (Tenders Electronic Daily) Search API.

For each company in the COMPANIES list, queries the TED Search API for
procurement notices mentioning that company, then writes structured results
to an Excel workbook with one sheet per company.

The Search API is fully anonymous – no API key or registration required.

API docs:  https://docs.ted.europa.eu/api/latest/search.html
Swagger:   https://api.ted.europa.eu/swagger
Query ref: https://ted.europa.eu/en/help/search-browse#expert-search
"""

from __future__ import annotations

import re
import sys
import time
import logging
from pathlib import Path
from typing import Any, Union

import pandas as pd
import requests
from openpyxl.styles import Font

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Companies to track.  Each entry is either a plain string (company name)
# or a dict with:
#   display_name  – used as the Excel sheet name
#   search_terms  – list of names/brands used in the FT query
#   org_numbers   – list of Swedish/Norwegian/Danish org numbers for
#                   precise role-detection (winner / buyer matching)
#
# Org numbers are normalised internally (dashes and spaces stripped)
# before comparison with TED identifier fields.
COMPANIES: list[Union[str, dict]] = [
    "Exsitec AB",
]

# TED API endpoint (anonymous, no auth needed)
API_URL = "https://api.ted.europa.eu/v3/notices/search"

# Fields to retrieve from the API.  Each adds to the "fields per page" budget
# (max 10 000 per page).  We keep a focused set of the most relevant fields.
API_FIELDS = [
    "publication-number",
    "notice-title",
    "notice-type",
    "publication-date",
    "dispatch-date",
    # Buyer
    "buyer-name",
    "buyer-country",
    "buyer-city",
    "buyer-identifier",
    # Winner / award
    "winner-name",
    "winner-country",
    "winner-identifier",
    "winner-selection-status",
    "contract-conclusion-date",
    # Values
    "estimated-value-lot",
    "estimated-value-cur-lot",
    "tender-value",
    "tender-value-cur",
    "total-value",
    "total-value-cur",
    # Procedure & classification
    "procedure-type",
    "contract-nature",
    "classification-cpv",
    "place-of-performance",
    # Deadlines
    "deadline-receipt-tender-date-lot",
    # Text
    "title-proc",
    "description-proc",
    # Tenderers
    "organisation-name-tenderer",
    # Links
    "links",
]

PAGE_SIZE = 100          # notices per API page (max 250)
REQUEST_DELAY = 0.5      # seconds between API calls (fair-use)
MAX_PAGES = 150          # safety limit (100 x 150 = 15 000 notices)

# Only fetch notices published on or after this date (YYYYMMDD).
# Reduces result size significantly for broad queries like Ambea.
MIN_PUBLICATION_DATE = "20210101"

# Columns to include in the Excel output (in this order).
# "Won by Company" is computed internally but intentionally excluded here;
# it is still used to apply green font formatting in the Excel writer.
OUTPUT_COLUMNS = [
    "Status",
    "Publication Date",
    "Awarded Value",
    "Buyer Name",
    "Estimated Value",
    "Winner Name",
    "Awarded Currency",
    "Estimated Currency",
]

# Paths
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = DATA_DIR / "ted_procurements.xlsx"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_text(value: Any, preferred_langs: tuple[str, ...] = ("eng", "swe")) -> str:
    """Extract a readable string from a TED API field value.

    TED returns multilingual text fields as ``{"swe": ["text"], "eng": ["text"]}``
    and simple fields as plain strings, lists, or numbers.
    """
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        # List of plain values (e.g. CPV codes, country codes)
        return ", ".join(str(v) for v in value)
    if isinstance(value, dict):
        # Multilingual dict - pick preferred language
        for lang in preferred_langs:
            if lang in value:
                v = value[lang]
                return ", ".join(str(i) for i in v) if isinstance(v, list) else str(v)
        # Fall back to first available language
        for lang, v in value.items():
            if isinstance(v, list):
                return ", ".join(str(i) for i in v)
            return str(v)
    return str(value)


def _extract_link(links_field: Any) -> str:
    """Return the English HTML link for a notice, or the first available."""
    if not isinstance(links_field, dict):
        return ""
    html = links_field.get("html", {})
    for lang in ("ENG", "SWE"):
        if lang in html:
            return html[lang]
    # First available
    for url in html.values():
        return url
    return ""


def _company_in_field(field_value: Any, company_name: str) -> bool:
    """Check if *company_name* appears (case-insensitive) in a field value."""
    text = _extract_text(field_value).lower()
    # Check both full name and name without corporate suffix
    name_lower = company_name.lower()
    core = re.sub(
        r"\s+(ab|as|a/s|gmbh|ltd|inc|oy|aps|bv|nv|sa|sl|srl|ag)\s*$",
        "", name_lower,
    ).strip()
    return name_lower in text or (core != name_lower and core in text)


def _parse_numeric(val: Any) -> object:
    """Try to extract a single numeric value; return None if unavailable.

    If the API returns a list of values (e.g. multiple lots), they are summed.
    The actual thousand-separator formatting is applied in the Excel writer.
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
    if isinstance(val, list):
        nums = []
        for v in val:
            try:
                nums.append(float(v))
            except (ValueError, TypeError):
                pass
        return sum(nums) if nums else None
    return None


def _normalize_org_num(num: str) -> str:
    """Normalise an org/company number to alphanumeric-only for comparison.

    Strips spaces and dashes so that ``556668-4345`` and ``5566684345`` are
    treated as equal.
    """
    return re.sub(r"[^0-9A-Za-z]", "", num)


def _normalize_company_config(company: Union[str, dict]) -> dict:
    """Return a unified config dict regardless of whether *company* is a
    plain string or an already-structured dict."""
    if isinstance(company, str):
        return {
            "display_name": company,
            "search_terms": [company],
            "org_numbers": set(),
        }
    org_numbers = {
        _normalize_org_num(n)
        for n in company.get("org_numbers", [])
        if n
    }
    return {
        "display_name": company["display_name"],
        "search_terms": company.get("search_terms", [company["display_name"]]),
        "org_numbers": org_numbers,
    }


def _org_in_identifier_field(field_value: Any, org_nums: set) -> bool:
    """Return True if any normalised org number in *org_nums* appears in
    *field_value* (a TED identifier field, possibly a list)."""
    if not org_nums or field_value is None:
        return False
    values = field_value if isinstance(field_value, list) else [field_value]
    for v in values:
        if _normalize_org_num(str(v)) in org_nums:
            return True
    return False


def _build_query(config: dict) -> str:
    """Build a TED expert-search query from *config*.

    Each search term becomes its own ``FT ~ "..."`` clause joined with OR.
    For a single term we also add a suffix-stripped variant inside parentheses
    (e.g. ``FT ~ ("Exsitec AB" OR "Exsitec")``).
    For multiple terms we chain separate FT clauses
    (e.g. ``FT ~ "Ambea" OR FT ~ "Nytida" OR ...``) because TED's parser
    does not reliably support large parenthesised OR lists inside a single
    FT ~ operator.

    Corporate suffixes are automatically stripped to broaden recall.
    """
    terms = config["search_terms"]

    if len(terms) == 1:
        # Single term: FT ~ ("Name" OR "CoreName") variant
        name = terms[0]
        core = re.sub(
            r"\s+(AB|AS|A/S|GmbH|Ltd|Inc|Oy|ApS|BV|NV|SA|SL|SRL|AG)\s*$",
            "", name, flags=re.IGNORECASE,
        ).strip()
        if core and core.lower() != name.lower():
            return f'FT ~ ("{name}" OR "{core}") AND PD >= {MIN_PUBLICATION_DATE}'
        return f'FT ~ "{name}" AND PD >= {MIN_PUBLICATION_DATE}'

    # Multiple terms: chain as separate FT clauses, wrapped in parens for the date AND
    clauses = [f'FT ~ "{t}"' for t in terms]
    ft_part = f'({" OR ".join(clauses)})'
    return f'{ft_part} AND PD >= {MIN_PUBLICATION_DATE}'


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

def search_notices(query: str, scope: str = "ALL") -> list[dict]:
    """Run a paginated search against the TED Search API.

    Parameters
    ----------
    query : str
        Expert-search query string.
    scope : str
        ``"ALL"`` for the full archive or ``"ACTIVE"`` for currently active.

    Returns
    -------
    list[dict]
        Raw notice dicts as returned by the API.
    """
    all_notices: list[dict] = []
    page = 1

    while page <= MAX_PAGES:
        payload = {
            "query": query,
            "page": page,
            "limit": PAGE_SIZE,
            "scope": scope,
            "fields": API_FIELDS,
            "onlyLatestVersions": True,
        }
        log.info("  Fetching page %d (scope=%s) ...", page, scope)
        for attempt in range(3):
            try:
                resp = requests.post(
                    API_URL,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=60,
                )
                resp.raise_for_status()
                break
            except requests.RequestException as exc:
                if attempt < 2:
                    log.warning("  Attempt %d failed (%s), retrying...", attempt + 1, exc)
                    time.sleep(2)
                else:
                    log.error("  API request failed after 3 attempts: %s", exc)
                    return all_notices

        data = resp.json()
        notices = data.get("notices", [])
        total = data.get("totalNoticeCount", 0)

        if page == 1:
            log.info("  Total matching notices: %s", total)

        if not notices:
            break

        all_notices.extend(notices)

        # Are we done?
        if isinstance(total, int) and len(all_notices) >= total:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return all_notices


# ---------------------------------------------------------------------------
# Data transformation
# ---------------------------------------------------------------------------

NOTICE_TYPE_LABELS = {
    # Planning
    "pin-buyer": "Prior Information Notice - Buyer Profile",
    "pin-only": "Prior Information Notice",
    "pin-cfc-standard": "Prior Information - Call for Competition",
    "pin-cfc-social": "Prior Information - Call for Competition (Social)",
    "pin-tran": "Prior Information - Public Transport",
    # Competition
    "cn-standard": "Contract Notice",
    "cn-social": "Contract Notice - Social",
    "cn-desg": "Design Contest Notice",
    "qu-sy": "Qualification System Notice",
    "subco": "Subcontracting Notice",
    # Result
    "can-standard": "Contract Award Notice",
    "can-social": "Contract Award Notice - Social",
    "can-desg": "Design Contest Result",
    "can-tran": "Contract Award - Public Transport",
    "can-modif": "Contract Modification Notice",
    "veat": "Voluntary Ex-Ante Transparency Notice",
}

PROCEDURE_TYPE_LABELS = {
    "open": "Open",
    "restricted": "Restricted",
    "neg-w-call": "Negotiated with Call",
    "neg-wo-call": "Negotiated without Call",
    "comp-dial": "Competitive Dialogue",
    "comp-neg": "Competitive Negotiation",
    "innovation": "Innovation Partnership",
    "oth-single": "Other (single stage)",
    "oth-multi": "Other (multi stage)",
}

# Notice types that indicate an ongoing/open procurement opportunity
_ACTIVE_NOTICE_TYPES = {
    "pin-buyer", "pin-only", "pin-cfc-standard", "pin-cfc-social", "pin-tran",
    "cn-standard", "cn-social", "cn-desg", "qu-sy", "subco",
}


def notices_to_dataframe(
    notices: list[dict],
    config: dict,
    active_pub_nums: set[str],
) -> pd.DataFrame:
    """Convert raw TED notices into a flat DataFrame.

    Parameters
    ----------
    notices : list[dict]
        Raw notice dicts from the API (full / ALL scope).
    config : dict
        Normalised company config (display_name, search_terms, org_numbers).
    active_pub_nums : set[str]
        Publication numbers that appeared in the ACTIVE scope query.
    """
    company = config["display_name"]
    org_numbers = config["org_numbers"]   # normalised set
    rows: list[dict] = []

    for n in notices:
        pub_num = _extract_text(n.get("publication-number"))
        notice_type_raw = _extract_text(n.get("notice-type"))
        winner_raw = _extract_text(n.get("winner-name"))
        tenderer_raw = _extract_text(n.get("organisation-name-tenderer"))

        # Determine company role.
        # When org numbers are provided, identifier-field matching takes
        # precedence over (and is added to) name-based matching.
        is_winner = (
            _company_in_field(n.get("winner-name"), company)
            or _org_in_identifier_field(n.get("winner-identifier"), org_numbers)
        )
        is_tenderer = _company_in_field(n.get("organisation-name-tenderer"), company)
        is_buyer = (
            _company_in_field(n.get("buyer-name"), company)
            or _org_in_identifier_field(n.get("buyer-identifier"), org_numbers)
        )

        if is_winner:
            role = "Winner"
        elif is_tenderer:
            role = "Tenderer"
        elif is_buyer:
            role = "Buyer"
        else:
            role = "Mentioned"

        # Status: use ACTIVE-scope membership first, then fall back to notice type
        if pub_num in active_pub_nums:
            status = "Active"
        elif notice_type_raw in _ACTIVE_NOTICE_TYPES:
            status = "Active"
        else:
            status = "Historical"

        # Best available value
        est_val = n.get("estimated-value-lot")
        est_cur = _extract_text(n.get("estimated-value-cur-lot"))
        award_val = n.get("tender-value") or n.get("total-value")
        award_cur = _extract_text(
            n.get("tender-value-cur") or n.get("total-value-cur")
        )

        rows.append({
            "Status": status,
            "Publication Date": _extract_text(n.get("publication-date", "")).split("+")[0],
            "Won by Company": "Yes" if is_winner else ("No" if winner_raw else ""),
            "Awarded Value": _parse_numeric(award_val),
            "Buyer Name": _extract_text(n.get("buyer-name")),
            "Estimated Value": _parse_numeric(est_val),
            "Winner Name": winner_raw,
            "Notice Type": NOTICE_TYPE_LABELS.get(notice_type_raw, notice_type_raw),
            "Title": (
                _extract_text(n.get("notice-title"))
                or _extract_text(n.get("title-proc"))
            ),
            "Description": _extract_text(n.get("description-proc"))[:500],
            "Publication Number": pub_num,
            "Awarded Currency": award_cur,
            "Estimated Currency": est_cur,
            "Tenderers": tenderer_raw,
            "Procedure Type": PROCEDURE_TYPE_LABELS.get(
                _extract_text(n.get("procedure-type")),
                _extract_text(n.get("procedure-type")),
            ),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # For companies tracked via org numbers:
    # keep only rows where the group won the contract OR the procurement is
    # still active.  Historical losses (Tenderer / Mentioned) are dropped.
    if config["org_numbers"]:
        df = df[(df["Status"] == "Active") | (df["Won by Company"] == "Yes")].copy()

    # Sort: Active on top, then by publication date descending
    df["_sort_status"] = df["Status"].map({"Active": 0, "Historical": 1})
    df["_sort_date"] = pd.to_datetime(df["Publication Date"], errors="coerce")
    df.sort_values(
        ["_sort_status", "_sort_date"],
        ascending=[True, False],
        inplace=True,
    )
    df.drop(columns=["_sort_status", "_sort_date"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


# ---------------------------------------------------------------------------
# Excel output
# ---------------------------------------------------------------------------

def _sanitise_sheet_name(name: str) -> str:
    """Excel sheet names must be <= 31 chars and cannot contain certain chars."""
    name = re.sub(r"[\\/*?\[\]:]", " ", name).strip()
    return name[:31]


def write_excel(company_frames: dict[str, pd.DataFrame]) -> None:
    """Write one sheet per company to the output Excel file."""
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        for company, df in company_frames.items():
            sheet = _sanitise_sheet_name(company)
            if df.empty:
                # Write a placeholder so the sheet still exists
                pd.DataFrame(
                    {"Info": [f"No procurement notices found for {company}"]}
                ).to_excel(writer, sheet_name=sheet, index=False)
            else:
                # Identify won rows using the internal column BEFORE subsetting
                won_row_indices: set[int] = set()
                if "Won by Company" in df.columns:
                    won_row_indices = set(
                        df.index[df["Won by Company"] == "Yes"].tolist()
                    )

                # Write only the defined output columns
                out_cols = [c for c in OUTPUT_COLUMNS if c in df.columns]
                df_out = df[out_cols]
                df_out.to_excel(writer, sheet_name=sheet, index=False)

                ws = writer.sheets[sheet]

                # Apply thousand-separator number format to value columns
                for col_name in ("Awarded Value", "Estimated Value"):
                    if col_name in df_out.columns:
                        col_idx = list(df_out.columns).index(col_name) + 1  # 1-based
                        for row_idx in range(2, len(df_out) + 2):  # skip header
                            cell = ws.cell(row=row_idx, column=col_idx)
                            if cell.value is not None:
                                cell.number_format = '#,##0'

                # Highlight rows where the company won with dark green text
                green_font = Font(color="006100")
                num_cols = len(df_out.columns)
                for df_idx in won_row_indices:
                    row_idx = df_idx + 2  # +1 for 1-based index, +1 for header
                    for col_idx in range(1, num_cols + 1):
                        ws.cell(row=row_idx, column=col_idx).font = green_font

                # Auto-size columns (approximate)
                for col_idx, col_name in enumerate(df_out.columns, 1):
                    max_len = max(
                        len(str(col_name)),
                        df_out[col_name].astype(str).str.len().max(),
                    )
                    # Cap at 60 characters width
                    adjusted = min(int(max_len) + 2, 60)
                    col_letter = ws.cell(1, col_idx).column_letter
                    ws.column_dimensions[col_letter].width = adjusted

    log.info("Wrote %s", OUTPUT_FILE)


def _build_winner_id_queries(org_numbers: set, batch_size: int = 25) -> list[str]:
    """Build one or more ``winner-identifier IN (...)`` queries for *org_numbers*.

    Splits into batches to stay well within TED's query-length limits.
    Each query is already suffixed with the MIN_PUBLICATION_DATE filter.
    """
    nums = list(org_numbers)
    queries = []
    for i in range(0, len(nums), batch_size):
        batch = nums[i : i + batch_size]
        id_list = " ".join(batch)
        queries.append(
            f"winner-identifier IN ({id_list}) AND PD >= {MIN_PUBLICATION_DATE}"
        )
    return queries


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=" * 60)
    log.info("TED Procurement Data Fetcher")
    log.info("=" * 60)

    company_frames: dict[str, pd.DataFrame] = {}

    for raw_company in COMPANIES:
        config = _normalize_company_config(raw_company)
        display_name = config["display_name"]
        log.info("Processing: %s", display_name)

        if config["org_numbers"]:
            # --- Identifier-based strategy (companies with org numbers) ---
            # Historical: query ONLY for notices where a subsidiary actually won.
            # This avoids fetching tens of thousands of false-positive FT matches.
            log.info(
                "  Using %d org numbers; querying winner-identifier directly",
                len(config["org_numbers"]),
            )
            id_queries = _build_winner_id_queries(config["org_numbers"])
            won_notices: list[dict] = []
            for q in id_queries:
                log.info("  Winner-ID query: %s", q)
                won_notices.extend(search_notices(q, scope="ALL"))
            log.info("  Retrieved %d won notices", len(won_notices))

            # Active: use FT query so we catch open tenders before a winner
            # is assigned (winner-identifier not yet populated).
            ft_query = _build_query(config)
            log.info("  FT query (active scope): %s", ft_query)
            active_notices = search_notices(ft_query, scope="ACTIVE")
            active_pub_nums = {
                _extract_text(n.get("publication-number"))
                for n in active_notices
            }
            log.info("  Active procurements found: %d", len(active_pub_nums))

            # Merge: active notices + won notices (deduplicate by pub number)
            seen: set[str] = set()
            all_notices: list[dict] = []
            for n in active_notices + won_notices:
                pn = _extract_text(n.get("publication-number"))
                if pn not in seen:
                    seen.add(pn)
                    all_notices.append(n)

        else:
            # --- Name-based strategy (e.g. Exsitec AB) ---
            query = _build_query(config)
            log.info("  Expert query: %s", query)

            all_notices = search_notices(query, scope="ALL")
            log.info("  Retrieved %d notices total", len(all_notices))

            active_raw = search_notices(query, scope="ACTIVE")
            active_pub_nums = {
                _extract_text(n.get("publication-number"))
                for n in active_raw
            }
            log.info("  Of which %d are currently active", len(active_pub_nums))

        # Build DataFrame
        df = notices_to_dataframe(all_notices, config, active_pub_nums)
        company_frames[display_name] = df
        log.info("  Done - %d rows for sheet '%s'", len(df), display_name)

    write_excel(company_frames)
    log.info("All done!")


if __name__ == "__main__":
    main()
