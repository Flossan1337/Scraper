#!/usr/bin/env python3
"""
track_privx_ct.py

Tracks which organisations run SSH Communications Security's PrivX (privileged
access management) on their own infrastructure, as a new-customer / churn
signal for SSH — independent of anything SSH publishes.

──────────────────────────────────────────────────────────────────────────────
What is actually being observed
──────────────────────────────────────────────────────────────────────────────
A self-hosted PrivX server is usually named privx.<company>.<tld> (or
privx01prod..., privx-dr..., privxpoc...). When the customer gives it a
publicly trusted TLS certificate, that certificate — hostname, issue date and
expiry — is published to the public Certificate Transparency (CT) logs. One
crt.sh query (q=privx.%) returns every logged certificate whose name begins
with "privx" (a leading "*." is ignored), across all of CT history:
~4,400 certificates on 2026-10-28, in ~10 s.

Because CT is an append-only historical record, there is no separate backfill:
every run returns the full history back to 2013, and the views rebuild "which
organisations held a valid PrivX certificate on date X" from the certificates'
own validity windows. A failed night loses nothing — the next run gets it all.

This is a SAMPLE of SSH's customer base, not a census:
  * Only installs with a PUBLIC certificate are visible. A PrivX server that is
    internal-only on a private CA (common for a PAM product) never appears.
  * Only hostnames that START with "privx". pam.acme.com or web.privx.acme.com
    are missed.
  * PrivX SaaS (<tenant>.live.saas.privx.io) is NOT covered and cannot be:
    wildcard certificate only, nothing in passive DNS, no DNSSEC to walk, and a
    27.6k-name guess sweep found zero tenants (investigated 2026-10-28).
  * Churn shows up late: a customer that stops is still "active" until its last
    certificate expires — up to ~13 months for a 1-year certificate, ~3 months
    for Let's Encrypt.
Use it for the timing of new logos and renewals that stop, not as a customer
count or an ARR estimate.

──────────────────────────────────────────────────────────────────────────────
Classification (the only manual part)
──────────────────────────────────────────────────────────────────────────────
Most names beginning "privx" are NOT PrivX: unrelated brands (privxy.com,
privxcy.com), random machine-generated labels, SSH's own demo/lab domains,
shared clouds where the owner can't be read off the domain (azure.com).
Each registrable domain therefore gets a category:

  1. data/privx_ct_review.json, if the domain is listed there — hand-edited,
     the script only reads it. Sets category (customer / partner / ssh_internal
     / shared_cloud / noise), the organisation name (so several domains can
     count as one org, e.g. sygnum.live + sygnum.tools), and a note.
  2. otherwise an automatic rule: 'noise' if the domain is itself a privx* name
     or no hostname reads as a PrivX server (INSTALL_LABEL), else 'unreviewed'.

Only customer + partner orgs are counted. 'unreviewed' is reported separately
until someone adds it to the review file. When a run discovers a new
unreviewed domain, it writes an alert that the workflow turns into a GitHub
issue (same mechanism as track_tu_brands.py).

──────────────────────────────────────────────────────────────────────────────
Outputs (all additive, same pattern as the other trackers in this repo)
──────────────────────────────────────────────────────────────────────────────
State file  : data/privx_ct_state.json   (every certificate ever seen — the
                                          durable copy if crt.sh loses data —
                                          plus when each domain was first seen)
Excel       : data/privx_ct.xlsx          ('Monthly' and 'Orgs' sheets, rebuilt
                                          from the state file on every run)
Postgres    : privx_ct_cert, privx_ct_domain; counts are SQL views,
              sql/views/privx_ct.sql
Metrics     : scripts/metrics/track_privx_ct.py.json  (for the CI summary)

The 'Monthly' sheet and privx_ct_monthly_v use the same definition: an org is
active on date D if any of its certificates has valid_from <= D <= valid_to
(UTC dates), evaluated at each month end (today for the current month).
"""
from __future__ import annotations

import argparse
import calendar
import json
import os
import re
import sys
import tempfile
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests

from core.cli import add_no_side_effects_flag, log_skip
from core.db import safe_insert, safe_upsert

# ── paths & constants ───────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
STATE_FILE = DATA_DIR / "privx_ct_state.json"
REVIEW_FILE = DATA_DIR / "privx_ct_review.json"
XLSX_PATH = DATA_DIR / "privx_ct.xlsx"
METRICS_DIR = REPO_ROOT / "scripts" / "metrics"

TZ = ZoneInfo("Europe/Stockholm")
CERT_TABLE = "privx_ct_cert"
DOMAIN_TABLE = "privx_ct_domain"

CRTSH_QUERY = "privx.%"
# crt.sh answers 502 under load fairly often (seen several times on
# 2026-10-28). Worst case 4 x 120 s + 3 x 20 s = 9 min, inside the 15-min CI cap.
FETCH_ATTEMPTS = 4
FETCH_TIMEOUT_S = 120
FETCH_RETRY_SLEEP_S = 20

COUNTED = ("customer", "partner")
CATEGORIES = {"customer", "partner", "ssh_internal", "shared_cloud", "noise"}

# A hostname's first label reads as a PrivX server if it is "privx", optionally
# followed by role words and digits (privx01dev, privxextcar02prod, privxpoc),
# optionally followed by a -suffix (privx-dr, privxpsql-ukw-db-dev2). Brussels
# Airport's 42 hosts are the reference set for the role words. 'p' only counts
# before a digit (privxp1), so privxmpp (an XMPP server) does not match.
ROLE_WORDS = ("dev|test|tst|prod|poc|uat|stg|staging|qa|demo|lab|dc|dr|db|app|"
              "proxy|prx|ext|car|nfs|web|pg|psql|postgresql|postgres|flex|"
              "portal|admin|lb|ha|mp|ca|cz|z|p(?=\\d)")
INSTALL_LABEL = re.compile(rf"^privx(?:{ROLE_WORDS}|\d{{1,3}})*(?:[-_][a-z0-9-]+)?$")
DNS_NAME = re.compile(r"(\*\.)?[a-z0-9_-]+(\.[a-z0-9_-]+)+")

# Second-level public suffixes seen in the data, so privx.cnel.gob.ec groups
# under cnel.gob.ec rather than gob.ec. A minimal stand-in for the Public Suffix
# List — add to it if a new country's names group wrongly.
TWO_LEVEL_SUFFIXES = {
    "co.uk", "org.uk", "ac.uk", "gov.uk", "com.au", "net.au", "org.au", "gov.au",
    "edu.au", "co.nz", "co.jp", "or.jp", "ne.jp", "go.jp", "ac.jp", "com.br",
    "gov.br", "com.cn", "com.hk", "com.sg", "com.tw", "com.my", "co.za", "com.tr",
    "co.in", "ac.in", "gov.in", "co.kr", "or.kr", "co.il", "ac.il", "com.mx",
    "gob.mx", "com.ar", "gob.ar", "gob.ec", "fin.ec", "com.ec", "edu.ec", "gov.ec",
    "ac.id", "co.id", "go.id", "or.id", "biz.id", "my.id", "web.id", "gv.at",
    "com.ua", "pp.ua", "com.pl",
}

SESSION = requests.Session()
SESSION.headers["User-Agent"] = (
    "Mozilla/5.0 (compatible; privx-ct-tracker/1.0; equity research; "
    "public CT logs only)"
)


# ── small helpers ───────────────────────────────────────────────────────────
def today_str() -> str:
    return datetime.now(TZ).date().isoformat()


def _rel(path: Path) -> str:
    """Display path relative to the repo root when possible, else as-is."""
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def first_label(host: str) -> str:
    return host.removeprefix("*.").split(".")[0]


def domain_of(host: str) -> str:
    """Registrable domain: last two labels, or three under a TWO_LEVEL_SUFFIXES."""
    labels = host.removeprefix("*.").split(".")
    if len(labels) >= 3 and ".".join(labels[-2:]) in TWO_LEVEL_SUFFIXES:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


# ── fetch ───────────────────────────────────────────────────────────────────
def fetch_crtsh() -> Optional[list[dict]]:
    """Every crt.sh row for q=privx.%, or None if crt.sh never answered. An
    empty 200 is treated as a failure too: crt.sh sometimes returns [] under
    load, and this query has never legitimately been empty."""
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            r = SESSION.get("https://crt.sh/",
                            params={"q": CRTSH_QUERY, "output": "json"},
                            timeout=FETCH_TIMEOUT_S)
            if r.status_code == 200:
                rows = r.json()
                if isinstance(rows, list) and rows:
                    return rows
                print(f"  crt.sh: HTTP 200 but no rows (attempt {attempt}/{FETCH_ATTEMPTS})")
            else:
                print(f"  crt.sh: HTTP {r.status_code} (attempt {attempt}/{FETCH_ATTEMPTS})")
        except Exception as e:
            print(f"  crt.sh: {type(e).__name__}: {e} (attempt {attempt}/{FETCH_ATTEMPTS})")
        if attempt < FETCH_ATTEMPTS:
            time.sleep(FETCH_RETRY_SLEEP_S)
    return None


def parse_rows(rows: list[dict]) -> dict[str, dict]:
    """{crt.sh id: cert} keeping only the privx* names on each certificate.
    A precertificate and its final certificate are separate CT entries with
    the same validity window; both are kept, which cannot change an org count."""
    certs: dict[str, dict] = {}
    for r in rows:
        hosts = sorted({
            n for n in (x.strip().lower() for x in r.get("name_value", "").split("\n"))
            if DNS_NAME.fullmatch(n) and first_label(n).startswith("privx")
        })
        if hosts:
            certs[str(r["id"])] = {
                "not_before": r["not_before"][:19],
                "not_after": r["not_after"][:19],
                "issuer": r.get("issuer_name"),
                "hosts": hosts,
            }
    return certs


# ── classification ──────────────────────────────────────────────────────────
def load_review() -> dict[str, dict]:
    if not REVIEW_FILE.exists():
        print(f"  review file {_rel(REVIEW_FILE)} missing — every domain auto-classified")
        return {}
    review = json.loads(REVIEW_FILE.read_text(encoding="utf-8")).get("domains", {})
    for dom, entry in list(review.items()):
        if entry.get("category") not in CATEGORIES:
            print(f"  review[{dom}]: unknown category {entry.get('category')!r} — ignored")
            del review[dom]
    return review


def classify(domain: str, hosts: set[str], review: dict[str, dict]) -> dict:
    """Category, org and reason for one registrable domain. Review file first,
    then the automatic rule (see the module docstring)."""
    if domain in review:
        e = review[domain]
        return {"org": e.get("org") or domain, "category": e["category"],
                "category_source": "review", "reason": None, "note": e.get("note")}
    if domain.startswith(("privx", "xn--privx")):
        cat, why = "noise", "domain is itself a privx* name (a brand, not a PrivX server)"
    elif not any(INSTALL_LABEL.match(first_label(h)) for h in hosts):
        cat, why = "noise", "no hostname reads as a PrivX server"
    else:
        cat, why = "unreviewed", "hostname reads as a PrivX server; not yet reviewed"
    return {"org": domain, "category": cat, "category_source": "auto",
            "reason": why, "note": None}


# ── org-level aggregation (mirrors sql/views/privx_ct.sql) ──────────────────
def build_orgs(certs: dict[str, dict], dom_info: dict[str, dict]) -> dict[tuple, dict]:
    """(org, category) -> domains, hosts and certificate validity windows."""
    orgs: dict[tuple, dict] = {}
    for c in certs.values():
        window = (c["not_before"][:10], c["not_after"][:10])
        for h in c["hosts"]:
            info = dom_info[domain_of(h)]
            o = orgs.setdefault((info["org"], info["category"]), {
                "domains": set(), "hosts": set(), "windows": set(),
                "source": info["category_source"], "note": info["note"]})
            o["domains"].add(domain_of(h))
            o["hosts"].add(h)
            o["windows"].add(window)
    for o in orgs.values():
        o["first_seen"] = min(nb for nb, _ in o["windows"])
        o["valid_until"] = max(na for _, na in o["windows"])
    return orgs


def active_on(o: dict, day: str) -> bool:
    return any(nb <= day <= na for nb, na in o["windows"])


def monthly_rows(orgs: dict[tuple, dict], run_date: str) -> list[list]:
    """One row per month from the first counted org's first month to now."""
    counted = {k: o for k, o in orgs.items() if k[1] in COUNTED}
    if not counted:
        return []
    today = date.fromisoformat(run_date)
    start = date.fromisoformat(min(o["first_seen"] for o in counted.values()))
    first_seen = {}
    for (org, _), o in counted.items():
        first_seen[org] = min(first_seen.get(org, o["first_seen"]), o["first_seen"])

    rows, prev_as_of, prev_active = [], None, set()
    y, m = start.year, start.month
    while (y, m) <= (today.year, today.month):
        as_of = min(date(y, m, calendar.monthrange(y, m)[1]), today).isoformat()
        active = {k for k, o in orgs.items() if active_on(o, as_of)}
        act_counted = {org for org, cat in active if cat in COUNTED}
        new = sorted(org for org, fs in first_seen.items()
                     if (prev_as_of is None or fs > prev_as_of) and fs <= as_of)
        lapsed = sorted(prev_active - act_counted)
        rows.append([
            f"{y}-{m:02d}", as_of,
            sum(1 for _, cat in active if cat == "customer"),
            sum(1 for _, cat in active if cat == "partner"),
            len(act_counted),
            len(new), ", ".join(new),
            len(lapsed), ", ".join(lapsed),
            sum(1 for _, cat in active if cat == "unreviewed"),
        ])
        prev_as_of, prev_active = as_of, act_counted
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return rows


# ── state ───────────────────────────────────────────────────────────────────
def load_state() -> Optional[dict]:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return None


def save_state(state: dict) -> None:
    STATE_FILE.write_text(
        json.dumps(state, indent=1, ensure_ascii=False, sort_keys=True), encoding="utf-8")


# ── main ────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_no_side_effects_flag(parser)
    args = parser.parse_args()

    run_date = today_str()
    print("=" * 70)
    print(f"[{run_date}] PrivX CT tracker — public Certificate Transparency logs")
    print("=" * 70)

    loaded = load_state()
    baseline = loaded is None
    state = loaded or {"version": 1, "baseline_date": run_date, "certs": {}, "domains": {}}
    certs: dict[str, dict] = state["certs"]
    print(f"Known certificates from state: {len(certs)}")

    # 1) FETCH — union with state; certificates are immutable, never removed
    print(f"\nFetching crt.sh q={CRTSH_QUERY} ...")
    t0 = time.time()
    rows = fetch_crtsh()
    fetch_ok = rows is not None
    new_certs = 0
    if fetch_ok:
        fetched = parse_rows(rows)
        for cid, c in fetched.items():
            if cid not in certs:
                certs[cid] = c
                new_certs += 1
        print(f"  {len(rows)} rows → {len(fetched)} certificates with privx* names "
              f"({time.time() - t0:.0f}s); {new_certs} new")
    else:
        print("  crt.sh unavailable — continuing from the state file; nothing is "
              "lost, the next run fetches the full history again")

    # 2) CLASSIFY — one category per registrable domain
    review = load_review()
    hosts_by_domain: dict[str, set[str]] = {}
    for c in certs.values():
        for h in c["hosts"]:
            hosts_by_domain.setdefault(domain_of(h), set()).add(h)
    dom_info = {d: classify(d, hs, review) for d, hs in hosts_by_domain.items()}

    seen_domains: dict[str, str] = state.setdefault("domains", {})
    new_domains = sorted(d for d in dom_info if d not in seen_domains)
    for d in new_domains:
        seen_domains[d] = run_date
    to_review = [] if baseline else [d for d in new_domains
                                     if dom_info[d]["category"] == "unreviewed"]
    stale_review = sorted(set(review) - set(dom_info))

    by_cat: dict[str, int] = {}
    for info in dom_info.values():
        by_cat[info["category"]] = by_cat.get(info["category"], 0) + 1

    # 3) AGGREGATE — orgs and today's counts
    orgs = build_orgs(certs, dom_info)
    active_today = {k for k, o in orgs.items() if active_on(o, run_date)}
    n_cust = sum(1 for _, cat in active_today if cat == "customer")
    n_part = sum(1 for _, cat in active_today if cat == "partner")
    n_unrev = sum(1 for _, cat in active_today if cat == "unreviewed")

    print(f"\nDomains by category : {dict(sorted(by_cat.items()))}")
    print(f"Active today        : {n_cust} customers + {n_part} partners "
          f"(+ {n_unrev} unreviewed, not counted)")
    print(f"New domains this run: {len(new_domains)}"
          + (" (baseline run — no alert)" if baseline and new_domains else ""))
    for d in to_review:
        print(f"  TO REVIEW: {d}  {', '.join(sorted(hosts_by_domain[d])[:4])}")
    if stale_review:
        print(f"  review entries with no certificate (typo?): {', '.join(stale_review)}")

    # 4) OUTPUTS
    if not args.no_side_effects:
        save_state(state)
        print(f"\nState saved → {_rel(STATE_FILE)} ({len(certs)} certificates)")
        write_excel(orgs, run_date)
    else:
        log_skip("state JSON and Excel workbook")

    if to_review:
        write_alert(to_review, hosts_by_domain, orgs, dom_info, run_date)

    cert_rows, dom_rows, db_err = write_to_db(certs, dom_info)

    write_metrics(run_date, fetch_ok, len(certs), new_certs, n_cust, n_part, n_unrev,
                  len(to_review), cert_rows, db_err)

    print("\nDatabas:")
    if db_err:
        print(f"  {CERT_TABLE}/{DOMAIN_TABLE}: MISSLYCKADES – {db_err}")
    else:
        print(f"  {CERT_TABLE}: {cert_rows} nya rader, {DOMAIN_TABLE}: {dom_rows} rader uppdaterade")
    print("\nDone.")


def write_to_db(certs: dict[str, dict], dom_info: dict[str, dict]):
    """Writes the FULL state every run, so a night whose DB write failed heals
    itself on the next. Certificates are immutable (ON CONFLICT DO NOTHING);
    domains are latest-known-state (upsert), because a review-file edit changes
    a domain's category. Never raises."""
    cert_rows = [
        (int(cid), h, domain_of(h), c["not_before"] + "Z", c["not_after"] + "Z", c["issuer"])
        for cid, c in certs.items() for h in c["hosts"]
    ]
    n_certs, err = safe_insert(
        CERT_TABLE, ["crtsh_id", "host", "domain", "not_before", "not_after", "issuer"],
        cert_rows, conflict_columns=["crtsh_id", "host"])
    if err:
        return None, None, err
    dom_rows = [(d, i["org"], i["category"], i["category_source"], i["reason"], i["note"])
                for d, i in dom_info.items()]
    n_doms, err = safe_upsert(
        DOMAIN_TABLE, ["domain", "org", "category", "category_source", "reason", "note"],
        dom_rows, conflict_columns=["domain"])
    return n_certs, n_doms, err


def write_alert(to_review, hosts_by_domain, orgs, dom_info, run_date) -> None:
    """Alert file for the workflow's issue-creation step (see daily.yml)."""
    alert_dir = Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir()))
    alert_file = alert_dir / "privx_ct_alert.json"
    lines = []
    for d in to_review:
        o = orgs[(dom_info[d]["org"], dom_info[d]["category"])]
        hosts = sorted(hosts_by_domain[d])
        lines.append(f"- **{d}** — first certificate {o['first_seen']}, valid until "
                     f"{o['valid_until']}: " + ", ".join(f"`{h}`" for h in hosts[:5])
                     + (f" (+{len(hosts) - 5} more)" if len(hosts) > 5 else "")
                     + f" — [crt.sh](https://crt.sh/?q={hosts[0].removeprefix('*.')})")
    body = (
        f"`track_privx_ct.py` found {len(to_review)} new domain(s) on {run_date} whose "
        "certificates look like a self-hosted PrivX server. They are **not counted** "
        "until reviewed.\n\n" + "\n".join(lines) + "\n\n"
        "To review: add each domain to `data/privx_ct_review.json` with a category "
        "(`customer`, `partner`, `ssh_internal`, `shared_cloud` or `noise`) and an "
        "`org` name, then close this issue."
    )
    title = f"PrivX CT: {len(to_review)} new domain(s) to review ({run_date}): " + \
            ", ".join(to_review[:5])
    alert_file.write_text(json.dumps({"title": title, "body": body}, ensure_ascii=False),
                          encoding="utf-8")
    print(f"Alert file written → {alert_file}")


def write_metrics(run_date, fetch_ok, certs_total, new_certs, n_cust, n_part, n_unrev,
                  n_to_review, db_rows, db_err) -> None:
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "date": run_date,
        "fetch_ok": fetch_ok,
        "certs": certs_total,
        "new_certs": new_certs,
        "active_customers": n_cust,
        "active_partners": n_part,
        "active_unreviewed": n_unrev,
        "to_review": n_to_review,
        "db_rows": db_rows if db_rows is not None else 0,
        "db_error": bool(db_err),
    }
    (METRICS_DIR / "track_privx_ct.py.json").write_text(json.dumps(payload), encoding="utf-8")


def write_excel(orgs: dict[tuple, dict], run_date: str) -> None:
    """Two sheets, both rebuilt from scratch on every run (nothing derived is
    stored incrementally, so a classification fix reaches the whole history):
    'Monthly' — the backfilled series; 'Orgs' — one row per organisation."""
    from openpyxl import Workbook
    from openpyxl.styles import Font

    wb = Workbook()
    ws = wb.active
    ws.title = "Monthly"
    ws.append(["Month", "As of", "Active customers", "Active partners", "Active orgs",
               "New orgs", "New org names", "Lapsed orgs", "Lapsed org names",
               "Active unreviewed"])
    for row in monthly_rows(orgs, run_date):
        ws.append(row)

    ws2 = wb.create_sheet("Orgs")
    ws2.append(["Org", "Category", "Category source", "Active today", "First seen",
                "Valid until", "Domains", "Hosts", "Sample hosts", "Note"])
    for (org, cat), o in sorted(orgs.items(), key=lambda kv: (kv[0][1], kv[1]["first_seen"])):
        ws2.append([org, cat, o["source"], active_on(o, run_date), o["first_seen"],
                    o["valid_until"], ", ".join(sorted(o["domains"])), len(o["hosts"]),
                    ", ".join(sorted(o["hosts"])[:3]), o["note"]])
    for sheet in (ws, ws2):
        for c in sheet[1]:
            c.font = Font(bold=True)

    wb.save(XLSX_PATH)
    print(f"Excel saved → {_rel(XLSX_PATH)}")


if __name__ == "__main__":
    main()
