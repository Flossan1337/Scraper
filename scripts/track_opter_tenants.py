#!/usr/bin/env python3
"""
track_opter_tenants.py

Tracks how many customers the TMS vendor Opter AB has, as a daily proxy /
lead indicator for its ARR — independent of anything Opter publishes.

──────────────────────────────────────────────────────────────────────────────
What is actually being observed
──────────────────────────────────────────────────────────────────────────────
Opter's cloud product ("Opters molnlösning") gives every customer its own
tenant on a shared platform, addressed as

    <companyslug><country>.opter.cloud      e.g. riksbudse.opter.cloud
                                                 jakobsentransno.opter.cloud

There is NO wildcard DNS on opter.cloud: a made-up name does not resolve, so a
name resolving == a real tenant exists. Every live tenant serves the Opter
login page (title "Opter") from one of two Azure IPs — 74.241.233.90 (the SE
cluster) and 20.251.106.235 (the NO cluster). This is 100 % public DNS data;
nothing here logs in, scrapes behind auth, or touches customer systems.

Self-hosted Opter installs (on the customer's own domain, e.g.
fleet.bdx.se/Opter/Account/Login) are NOT on opter.cloud and are not captured
here — see the summary doc / KNOWN limitations below.

──────────────────────────────────────────────────────────────────────────────
How the daily run works (two independent jobs)
──────────────────────────────────────────────────────────────────────────────
1. RE-CHECK (cheap, always runs, catches churn): resolve every host we already
   know about (from the state file) in DNS. A host that stops resolving is a
   candidate churn/loss; a transient DNS error is NOT treated as a loss.

2. DISCOVERY (heavier, best-effort, catches adds): find new tenants two ways —
     a) passive DNS databases for *.opter.cloud (HackerTarget, crt.sh, urlscan,
        RapidDNS, subdomain.center) — direct, but only ~half of tenants are
        ever indexed there.
     b) register-guessing: pull the national company registers, filter to
        transport/courier/logistics industry codes, generate candidate slugs
        from company names, and DNS-check them. This is what finds the other
        half. Sources:
          SE  SCB "värdefulla datamängder" bulk file (weekly HVD open data),
              via a public mirror because bolagsverket.se CAPTCHAs scripts
          NO  Brønnøysundregistrene full entity bulk download
          FI  PRH/YTJ avoindata all-companies bulk file
          DK  placeholder — needs free CVR system-til-system credentials
              (email cvrselvbetjening@erst.dk). Wired up but inert until the
              env vars are set; the rest of the run does not depend on it.

Every discovery source is wrapped so that a slow/broken/rate-limited source
logs one line and is skipped — the re-check job and the DB/Excel writes always
complete. The script never exits non-zero on a data-source failure and never
raises out of a DB write (see CLAUDE.md).

──────────────────────────────────────────────────────────────────────────────
Outputs (all additive, same pattern as the other trackers in this repo)
──────────────────────────────────────────────────────────────────────────────
State file  : data/opter_tenants_state.json   (durable universe: every host we
                                               have ever seen + first/last_seen)
Excel       : data/opter_tenants.xlsx          (Daily summary + Tenants sheets)
Postgres    : opter_tenant_snapshot            (one row per live tenant per day;
                                               counts/adds/losses are a SQL view,
                                               sql/views/opter_tenants.sql)
Metrics     : scripts/metrics/track_opter_tenants.py.json  (for the CI summary)

DB access goes through core.db.safe_insert (never raises). The snapshot table is
the usual (snapshot_date, host) ON CONFLICT DO NOTHING shape, so reruns on the
same day are no-ops.
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import re
import socket
import sys
import time
import unicodedata
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

import requests

from core.cli import add_no_side_effects_flag, log_skip
from core.db import safe_insert

# ── paths & constants ───────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
STATE_FILE = DATA_DIR / "opter_tenants_state.json"
XLSX_PATH = DATA_DIR / "opter_tenants.xlsx"
METRICS_DIR = REPO_ROOT / "scripts" / "metrics"

BASE_DOMAIN = "opter.cloud"
TZ = ZoneInfo("Europe/Stockholm")
DB_TABLE = "opter_tenant_snapshot"

# Azure IPs Opter serves live tenants from. Used only to label rows; a tenant
# on an unexpected IP is still counted (Opter could add clusters).
KNOWN_CLUSTER_IPS = {"74.241.233.90": "SE", "20.251.106.235": "NO"}

# Transport / courier / logistics / removals industry codes. Same intent across
# registers even though the code systems differ slightly (all NACE rev.2 based).
NACE_SE = {"49410", "49420", "53200", "52291", "52292", "52100", "49390"}
NACE_NO = {"49.410", "49.420", "53.200", "52.291", "52.292", "52.100", "49.390"}
NACE_FI = {"49410", "49420", "53200", "52291", "52292", "52100", "49390"}

# Words that carry no identifying information in a haulier's name — stripped so
# the distinctive part of the name drives the slug guess.
STOP_WORDS = {
    "as", "asa", "ab", "sa", "da", "ans", "ba", "nuf", "enk", "og", "and", "the",
    "publ", "hb", "kb", "aktiebolag", "oy", "oyj", "ky", "tmi", "ay", "ltd", "ja",
    "aps", "a/s", "i", "&",
}
GENERIC_WORDS = {
    "transport", "transporter", "trans", "logistik", "logistikk", "logistics",
    "logistiikka", "spedition", "spedisjon", "speditsjon", "frakt", "rahti",
    "bud", "budservice", "budbil", "cargo", "service", "auto", "bil", "maskin",
    "vare", "flytting", "flytt", "muutto", "kuljetus", "kuljetukset",
    "kuljetusliike", "kuljetuspalvelu", "huolinta", "express", "akeri", "åkeri",
}
SLUG_ABBR = [("transport", "trans"), ("transport", "trp"),
             ("logistik", "log"), ("logistics", "log"),
             ("kuljetus", "kulj"), ("logistiikka", "log")]

# Hosts that exist on opter.cloud but are Opter-internal, not customers.
INTERNAL_SLUGS = {"opter", "perrademo", "playground", "www"}

# Per-source network budgets (seconds). A source that overruns is skipped.
SESSION = requests.Session()
SESSION.headers["User-Agent"] = (
    "Mozilla/5.0 (compatible; opter-tenant-tracker/1.0; equity research; "
    "public DNS only)"
)
socket.setdefaulttimeout(5)


# ── small helpers ───────────────────────────────────────────────────────────
def today_str() -> str:
    return datetime.now(TZ).date().isoformat()


def _rel(path: Path) -> str:
    """Display path relative to the repo root when possible, else as-is."""
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def country_of(slug: str) -> Optional[str]:
    m = re.search(r"(se|no|fi|dk|ee)$", slug)
    return m.group(1) if m else None


def _ascii_words(name: str) -> list[str]:
    s = name.lower().replace("æ", "ae").replace("ø", "o").replace("ß", "ss")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return [w for w in re.findall(r"[a-z0-9]+", s) if w not in STOP_WORDS]


def slug_candidates(name: str) -> set[str]:
    """Generate plausible opter.cloud slug stems (without country suffix) from a
    company name. Mirrors how Opter's onboarding seems to abbreviate names:
    whole name, first word, first two words, initials, and with the generic
    words dropped or shortened."""
    w = _ascii_words(name)
    if not w:
        return set()
    joined = "".join(w)
    core = [x for x in w if x not in GENERIC_WORDS] or w
    out = {joined, w[0], "".join(w[:2]), "".join(core), core[0], "".join(core[:2])}
    out.update(core)  # each distinctive word alone
    for a, b in SLUG_ABBR:
        out.add(joined.replace(a, b))
    for g in w:
        if g in GENERIC_WORDS:
            out.add("".join(core) + g[:4])
    if len(w) > 1:
        out.add("".join(x[0] for x in w))  # pure initials
    return {v for v in out if 3 <= len(v) <= 30}


def resolve_host(host: str, retries: int = 3) -> tuple[str, Optional[str], str]:
    """Return (host, ip_or_None, status) where status is 'live' | 'dead' |
    'error'. 'dead' is a confident NXDOMAIN; 'error' is a transient failure and
    must never be counted as a lost customer."""
    last_exc = None
    for _ in range(retries):
        try:
            return host, socket.gethostbyname(host), "live"
        except socket.gaierror as e:
            last_exc = e
            # errno differs per platform; treat repeated resolution failure as
            # 'dead' only after retries, otherwise 'error'.
            time.sleep(0.3)
        except Exception as e:  # timeout, temporary DNS failure, …
            last_exc = e
            time.sleep(0.3)
    # Distinguish confident NXDOMAIN from transient noise as best we can.
    msg = str(last_exc).lower()
    if "not known" in msg or "nxdomain" in msg or "no such host" in msg or \
       "name or service not known" in msg or "11001" in msg:
        return host, None, "dead"
    return host, None, "error"


def resolve_many(hosts: Iterable[str], workers: int = 24) -> dict[str, tuple[Optional[str], str]]:
    hosts = list(dict.fromkeys(hosts))
    if not hosts:
        return {}
    res: dict[str, tuple[Optional[str], str]] = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for host, ip, status in ex.map(resolve_host, hosts):
            res[host] = (ip, status)
    return res


def slugs_to_hosts(slugs: Iterable[str], suffix: str) -> list[str]:
    return [f"{s}{suffix}.{BASE_DOMAIN}" for s in slugs]


# ── discovery: passive DNS ──────────────────────────────────────────────────
def discover_passive_dns() -> set[str]:
    """Names already indexed by public passive-DNS / CT sources. Direct but
    partial. Each source is independent and best-effort."""
    found: set[str] = set()
    rx = re.compile(r"[a-z0-9][a-z0-9-]*\.opter\.cloud")

    def grab(label: str, fn) -> None:
        try:
            n = fn()
            found.update(n)
            print(f"  passive[{label}]: {len(n)} names")
        except Exception as e:
            print(f"  passive[{label}]: skipped ({type(e).__name__}: {e})")

    grab("hackertarget", lambda: {
        line.split(",")[0].strip().lower()
        for line in SESSION.get(
            f"https://api.hackertarget.com/hostsearch/?q={BASE_DOMAIN}", timeout=30
        ).text.splitlines() if ".opter.cloud" in line
    })
    grab("crtsh", lambda: {
        n.lower() for r in SESSION.get(
            f"https://crt.sh/?q=%25.{BASE_DOMAIN}&output=json", timeout=45
        ).json() for n in r["name_value"].split("\n")
    })
    grab("urlscan", lambda: set(rx.findall(SESSION.get(
        f"https://urlscan.io/api/v1/search/?q=domain:{BASE_DOMAIN}&size=10000",
        timeout=30).text.lower())))
    grab("rapiddns", lambda: set(rx.findall(SESSION.get(
        f"https://rapiddns.io/subdomain/{BASE_DOMAIN}?full=1", timeout=30).text.lower())))
    grab("subdomaincenter", lambda: set(rx.findall(SESSION.get(
        f"https://api.subdomain.center/?domain={BASE_DOMAIN}", timeout=45).text.lower())))

    # normalise to bare host, drop wildcards/blanks
    return {h for h in found if h.endswith(f".{BASE_DOMAIN}") and "*" not in h}


# ── discovery: register guessing ────────────────────────────────────────────
def _guess_from_names(names: Iterable[str], suffix: str, label: str) -> set[str]:
    cands: set[str] = set()
    for nm in names:
        cands |= slug_candidates(nm)
    hosts = slugs_to_hosts(cands, suffix)
    res = resolve_many(hosts, workers=64)
    hits = {h for h, (ip, st) in res.items() if st == "live"}
    print(f"  register[{label}]: {len(cands)} slug candidates → {len(hits)} live")
    return hits


def discover_sweden() -> set[str]:
    """SCB bulk company file (SNI codes in Ng1..Ng5). bolagsverket.se blocks
    scripted downloads with a CAPTCHA, so we read SCB's file from a public
    weekly mirror. If the mirror is unavailable, Sweden discovery is skipped for
    the day — the re-check job still catches Swedish churn."""
    # newest snapshot in the mirror
    tree = SESSION.get(
        "https://huggingface.co/api/datasets/krafs/bolagsverket-arkiv/tree/main/ra/scb_bulkfil",
        timeout=30).json()
    latest = sorted(x["path"] for x in tree if x["type"] == "directory")[-1]
    url = f"https://huggingface.co/datasets/krafs/bolagsverket-arkiv/resolve/main/{latest}/scb_bulkfil.zip"
    raw = SESSION.get(url, timeout=180).content
    names: list[str] = []
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        fn = [n for n in z.namelist() if n.endswith(".txt")][0]
        import csv
        with z.open(fn) as fh:
            text = io.TextIOWrapper(fh, encoding="iso-8859-1", newline="")
            for row in csv.DictReader(text, delimiter="\t", quoting=csv.QUOTE_NONE):
                if row.get("FtgStat") != "1":  # only active
                    continue
                if {row.get(f"Ng{i}") for i in range(1, 6)} & NACE_SE:
                    for key in ("Namn", "Foretagsnamn"):
                        if row.get(key):
                            names.append(row[key])
    print(f"  register[SE]: {len(names)} active transport company names")
    return _guess_from_names(names, "se", "SE")


def discover_norway() -> set[str]:
    """Brønnøysundregistrene full entity bulk download (the paged API caps at
    10k results; the bulk file is the whole register)."""
    url = "https://data.brreg.no/enhetsregisteret/api/enheter/lastned"
    raw = SESSION.get(
        url, timeout=240,
        headers={"Accept": "application/vnd.brreg.enhetsregisteret.enhet.v2+gzip;charset=UTF-8"},
    ).content
    data = json.load(gzip.open(io.BytesIO(raw)))
    names: list[str] = []
    for e in data:
        codes = {e.get(k, {}).get("kode") for k in
                 ("naeringskode1", "naeringskode2", "naeringskode3") if e.get(k)}
        if codes & NACE_NO and not e.get("konkurs") and not e.get("underAvvikling") \
                and not e.get("slettedato"):
            names.append(e["navn"])
    print(f"  register[NO]: {len(names)} active transport company names")
    return _guess_from_names(names, "no", "NO")


def discover_finland() -> set[str]:
    """PRH/YTJ avoindata all-companies bulk zip. Uses every current registered
    name (incl. parallel Swedish names)."""
    url = "https://avoindata.prh.fi/opendata-ytj-api/v3/all_companies"
    raw = SESSION.get(url, timeout=180).content
    names: list[str] = []
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        fn = [n for n in z.namelist() if n.endswith(".json")][0]
        companies = json.load(z.open(fn))
    if isinstance(companies, dict):
        companies = companies.get("companies") or next(
            (v for v in companies.values() if isinstance(v, list)), [])
    for c in companies:
        # In this bulk file the NACE code lives in mainBusinessLine.type
        # (e.g. "49410"), NOT ".code" — confirmed against data_YYYYMMDD.json.
        mbl = c.get("mainBusinessLine")
        code = mbl.get("type") if isinstance(mbl, dict) else None
        if code not in NACE_FI:
            continue
        for n in c.get("names", []):
            if isinstance(n, dict) and not n.get("endDate") and n.get("name"):
                names.append(n["name"])
    print(f"  register[FI]: {len(names)} active transport company names")
    return _guess_from_names(names, "fi", "FI")


def discover_denmark() -> set[str]:
    """PLACEHOLDER for Denmark. Denmark's CVR register is free but not
    self-service: email cvrselvbetjening@erst.dk for system-til-system
    (Elasticsearch) credentials, then set CVR_USERNAME / CVR_PASSWORD as GitHub
    secrets. Until both are present this returns nothing and the rest of the run
    is unaffected."""
    user, pw = os.environ.get("CVR_USERNAME"), os.environ.get("CVR_PASSWORD")
    if not (user and pw):
        print("  register[DK]: skipped (no CVR credentials — see docstring)")
        return set()
    # Implementation once credentials exist: query the CVR Elasticsearch
    # distribution endpoint for companies on the relevant branchekoder, collect
    # names, then _guess_from_names(names, "dk", "DK"). Left unimplemented on
    # purpose so no unverified request shape is baked in before we can test it.
    try:
        endpoint = os.environ.get(
            "CVR_ENDPOINT",
            "http://distribution.virk.dk/cvr-permanent/_search")
        query = {
            "size": 0,
            "query": {"terms": {
                "Vrvirksomhed.virksomhedMetadata.nyesteHovedbranche.branchekode":
                    ["494100", "532000", "522910", "522920", "521000", "493900"]}},
        }
        r = SESSION.post(endpoint, json=query, auth=(user, pw), timeout=60)
        r.raise_for_status()
        print("  register[DK]: credentials present — implement paging in "
              "discover_denmark() to harvest names")
    except Exception as e:
        print(f"  register[DK]: skipped ({type(e).__name__}: {e})")
    return set()


DISCOVERY_SOURCES = [
    ("passive_dns", discover_passive_dns),
    ("register_se", discover_sweden),
    ("register_no", discover_norway),
    ("register_fi", discover_finland),
    ("register_dk", discover_denmark),
]


def run_discovery() -> dict[str, str]:
    """Run every discovery source, best-effort. Returns {host: source}. A source
    that raises or overruns is logged and skipped; discovery failing entirely is
    fine — the re-check job below still runs."""
    discovered: dict[str, str] = {}
    for label, fn in DISCOVERY_SOURCES:
        t0 = time.time()
        try:
            hits = fn()
        except Exception as e:
            print(f"  discovery[{label}]: FAILED ({type(e).__name__}: {e}) — skipping")
            continue
        for h in hits:
            discovered.setdefault(h, label)
        print(f"  discovery[{label}]: {len(hits)} hosts in {time.time()-t0:.0f}s")
    return discovered


# ── state ───────────────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {"version": 1, "baseline_date": today_str(), "tenants": {}}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(
        json.dumps(state, indent=1, ensure_ascii=False), encoding="utf-8")


# ── main ────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_no_side_effects_flag(parser)
    args = parser.parse_args()

    run_date = today_str()
    print("=" * 70)
    print(f"[{run_date}] Opter tenant tracker — public opter.cloud DNS")
    print("=" * 70)

    state = load_state()
    tenants: dict[str, dict] = state.setdefault("tenants", {})
    known_hosts = set(tenants)
    print(f"Known universe from state: {len(known_hosts)} hosts")

    # 1) DISCOVERY — new candidate hosts (adds)
    print("\nDiscovery (best-effort):")
    discovered = run_discovery()
    new_candidates = {h: s for h, s in discovered.items()
                      if h.split(".")[0] not in INTERNAL_SLUGS}
    print(f"Discovery returned {len(new_candidates)} hosts "
          f"({len(set(new_candidates) - known_hosts)} not previously known)")

    # 2) RE-CHECK — resolve the full universe (known ∪ discovered)
    check_hosts = sorted(known_hosts | set(new_candidates))
    print(f"\nResolving {len(check_hosts)} hosts ...")
    resolution = resolve_many(check_hosts, workers=48)

    live_today: list[dict] = []
    adds, losses, errors = [], [], 0
    for host in check_hosts:
        ip, status = resolution.get(host, (None, "error"))
        slug = host.split(".")[0]
        rec = tenants.get(host)
        if rec is None:  # newly discovered host
            rec = {"slug": slug, "country": country_of(slug),
                   "first_seen": run_date, "last_seen": None, "last_ip": None,
                   "is_live": False, "source": new_candidates.get(host, "unknown")}
            tenants[host] = rec

        if status == "live":
            was_live = rec.get("is_live")
            rec.update(last_seen=run_date, last_ip=ip, is_live=True)
            if rec["first_seen"] == run_date and rec["source"] != "seed":
                adds.append(host)
            live_today.append({"host": host, "slug": slug,
                               "country": rec["country"], "ip": ip,
                               "source": rec["source"]})
            rec.pop("lost_date", None)
        elif status == "dead":
            if rec.get("is_live"):
                rec["is_live"] = False
                rec["lost_date"] = run_date
                losses.append(host)
            # confirmed-dead hosts are kept in state as history, not re-added
            # to the daily snapshot.
        else:  # transient error — do NOT treat as a loss; carry prior state
            errors += 1
            if rec.get("is_live"):
                # keep counting it as live today using last known ip, so a DNS
                # blip doesn't create phantom churn in the snapshot table
                live_today.append({"host": host, "slug": slug,
                                   "country": rec["country"],
                                   "ip": rec.get("last_ip"),
                                   "source": rec["source"]})

    by_country: dict[str, int] = {}
    for t in live_today:
        by_country[t["country"] or "??"] = by_country.get(t["country"] or "??", 0) + 1

    print(f"\nLive tenants today : {len(live_today)}")
    print(f"  by country       : {dict(sorted(by_country.items()))}")
    print(f"  adds today       : {len(adds)} {adds if adds else ''}")
    print(f"  losses today     : {len(losses)} {losses if losses else ''}")
    print(f"  transient errors : {errors} (carried as live, not counted as loss)")

    # 3) OUTPUTS
    if not args.no_side_effects:
        save_state(state)
        print(f"\nState saved → {_rel(STATE_FILE)} "
              f"({len(tenants)} hosts total)")
        write_excel(state, run_date, len(live_today), by_country, len(adds), len(losses))
    else:
        log_skip("state JSON and Excel workbook")

    db_rows, db_err = write_snapshot_to_db(run_date, live_today)

    write_metrics(run_date, len(live_today), by_country, len(adds), len(losses),
                  db_rows, db_err)

    print("\nDatabas:")
    if db_err:
        print(f"  {DB_TABLE}: MISSLYCKADES – {db_err}")
    elif db_rows is None:
        print(f"  {DB_TABLE}: hoppades över (ingen data att skriva)")
    else:
        print(f"  {DB_TABLE}: {db_rows} rader skrivna")
    print("\nDone.")


def write_snapshot_to_db(run_date: str, live_today: list[dict]):
    """One row per live tenant for run_date. Snapshot shape: ON CONFLICT
    (snapshot_date, host) DO NOTHING, so reruns are no-ops. Never raises."""
    if not live_today:
        return None, None
    columns = ["snapshot_date", "host", "slug", "country", "ip", "resolved", "source"]
    rows = [
        (run_date, t["host"], t["slug"], t["country"], t["ip"],
         t["ip"] is not None, t["source"])
        for t in live_today
    ]
    return safe_insert(DB_TABLE, columns, rows, conflict_columns=["snapshot_date", "host"])


def write_metrics(run_date, live_count, by_country, adds, losses, db_rows, db_err):
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "date": run_date,
        "live_tenants": live_count,
        "adds": adds,
        "losses": losses,
        **{f"live_{k}": v for k, v in sorted(by_country.items())},
        "db_rows": db_rows if db_rows is not None else 0,
        "db_error": bool(db_err),
    }
    (METRICS_DIR / "track_opter_tenants.py.json").write_text(
        json.dumps(payload), encoding="utf-8")


def write_excel(state, run_date, live_count, by_country, adds, losses):
    """Two sheets: 'Daily summary' (one row per run date, idempotent for today)
    and 'Tenants' (current universe with first/last seen). Power Query in the
    dashboard reads these; the DB view is the analytical source."""
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import Font

    if XLSX_PATH.exists():
        wb = load_workbook(XLSX_PATH)
    else:
        wb = Workbook()
        wb.remove(wb.active)

    # ── Daily summary ──
    if "Daily summary" in wb.sheetnames:
        ws = wb["Daily summary"]
    else:
        ws = wb.create_sheet("Daily summary")
        headers = ["Date", "Live tenants", "Adds", "Losses",
                   "SE", "NO", "FI", "DK", "EE", "Other"]
        ws.append(headers)
        for c in ws[1]:
            c.font = Font(bold=True)
    cc = dict(by_country)
    summary_row = [run_date, live_count, adds, losses,
                   cc.get("se", 0), cc.get("no", 0), cc.get("fi", 0),
                   cc.get("dk", 0), cc.get("ee", 0), cc.get("??", 0)]
    # idempotent: replace an existing row for today, else append
    replaced = False
    for row in ws.iter_rows(min_row=2):
        if row[0].value == run_date:
            for cell, val in zip(row, summary_row):
                cell.value = val
            replaced = True
            break
    if not replaced:
        ws.append(summary_row)

    # ── Tenants (full current universe) ──
    if "Tenants" in wb.sheetnames:
        wb.remove(wb["Tenants"])
    ws2 = wb.create_sheet("Tenants")
    ws2.append(["Host", "Slug", "Country", "First seen", "Last seen",
                "Is live", "Lost date", "Last IP", "Source"])
    for c in ws2[1]:
        c.font = Font(bold=True)
    for host, rec in sorted(state["tenants"].items()):
        ws2.append([host, rec["slug"], rec.get("country"), rec["first_seen"],
                    rec.get("last_seen"), rec.get("is_live"),
                    rec.get("lost_date"), rec.get("last_ip"), rec.get("source")])

    wb.save(XLSX_PATH)
    print(f"Excel saved → {_rel(XLSX_PATH)}")


if __name__ == "__main__":
    main()
