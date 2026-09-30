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

2. DISCOVERY (heavier, best-effort, catches adds): find new tenants three ways —
     a) passive DNS databases for *.opter.cloud (HackerTarget, crt.sh, urlscan,
        RapidDNS, subdomain.center) — direct, but only ~half of tenants are
        ever indexed there.
     b) the Wayback Machine CDX index, which is a public wildcard-queryable
        list of every URL the Internet Archive has ever archived, so it answers
        "which *.opter.cloud hosts have existed?" outright. Best yield per
        second of any source, and the only one that reaches markets we have no
        register for (DK, EE) — see discover_wayback().
     c) register-guessing: pull the national company registers, keep the firms
        that look like hauliers, generate candidate slugs from their names, and
        DNS-check them. This is what finds the other half.

        "Looks like a haulier" is deliberately two tests OR'd together, because
        the industry code alone is not good enough: real Opter customers are
        registered as holding companies, construction firms and wholesalers.
        So we keep a firm on a transport NACE code (NACE_TRANSPORT) OR one
        whose NAME reads as transport whatever its code (is_haulier_name()).
        Being loose costs only DNS lookups and can never corrupt a number — a
        non-customer has no DNS record. Sources:
          SE  SCB "värdefulla datamängder" bulk file (weekly HVD open data),
              via a public mirror because bolagsverket.se CAPTCHAs scripts
          NO  Brønnøysundregistrene full entity bulk download
          FI  PRH/YTJ avoindata all-companies bulk file
          DK  placeholder — needs free CVR system-til-system credentials
              (email cvrselvbetjening@erst.dk). Wired up but inert until the
              env vars are set; the rest of the run does not depend on it.

Every discovery source is wrapped so that a slow/broken/rate-limited source
logs one line and is skipped, and the discovery phase as a whole is capped by
DISCOVERY_BUDGET_S — the re-check job and the DB/Excel writes always complete.
The script never exits non-zero on a data-source failure and never raises out of
a DB write (see CLAUDE.md).

Note the two DNS paths, which is the difference between this script working and
not: the candidate sweep uses probe_host() (one shot, ~1570 hosts/s) while the
re-check of known hosts uses resolve_host() (retries, so a blip cannot fake a
churn event). Using the retrying path for the sweep put a full run at ~77 min
against a 25-min CI cap, so the step was killed before any write and the tracker
produced nothing at all for its first 11 nights.

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

# Transport / courier / logistics / removals industry codes, as canonical NACE
# rev.2. All four registers use NACE-derived codes but format them differently,
# so the per-register variants are DERIVED rather than maintained by hand —
# adding a code below reaches every country at once.
#
# Codes past the original six are transport-adjacent activities that hauliers
# and courier firms genuinely register under (cargo handling, taxi/budbil,
# truck rental, postal). They widen the net; the DNS check is what decides.
NACE_TRANSPORT = {
    "49.410",  # road freight transport
    "49.420",  # removals / flyttjänster
    "49.390",  # other passenger land transport
    "49.310",  # urban and suburban passenger land transport
    "49.320",  # taxi operation — many budbil/courier firms sit here
    "52.100",  # warehousing and storage
    "52.240",  # cargo handling
    "52.291",  # freight forwarding / spedition
    "52.292",  # other transport support / shipping agency
    "52.290",  # other transport support activities (parent code)
    "53.100",  # postal activities under universal service obligation
    "53.200",  # other postal and courier activities
    "77.120",  # renting and leasing of trucks
}
NACE_DOTTED = set(NACE_TRANSPORT)                                 # NO (brreg)
NACE_PLAIN = {c.replace(".", "") for c in NACE_TRANSPORT}         # SE (SNI), FI (PRH)
NACE_DK = {c.replace(".", "") + "0" for c in NACE_TRANSPORT}      # DK (DB07, 6-digit)

# A company's industry code is NOT a reliable filter for Opter's customer base:
# plenty of real hauliers are registered as holding companies, construction
# firms or wholesalers. So on top of the codes above we keep any firm whose
# NAME reads like a transport business, whatever code it filed under. Verified
# examples this recovers: AMK Transport AB, BHS Logistics AB, GNS Cargo AS —
# all three are live opter.cloud tenants that the code filter alone misses.
#
# Being generous here is cheap and safe: a name-matched firm that is not an
# Opter customer simply has no DNS record, so a false positive costs a DNS
# lookup and can never produce a wrong number. The only budget is run time.
#
# Long, distinctive stems are matched ANYWHERE in a word so Scandinavian
# compounds land ("Vinstaåkeri", "Stadsbudet"); short or ambiguous ones must be
# a whole word, so "Transcendent AB" or "Budget Sport AB" are not dragged in.
NAME_HINT_STEMS = (
    "transpor", "logisti", "spedit", "spedis", "akeri", "budbil", "budservice",
    "kuljetus", "huolinta", "flytting", "flyttebyra", "distribu", "lastebil",
    "lastbil", "godstrafik", "kurier", "kurir", "courier", "haulage",
)
# 'akeri' is the one stem with a common innocent host word around it: nearly
# every Norwegian bakery is an "X Bakeri". Excluded so the SE/NO sweeps don't
# spend thousands of lookups on bakeries.
NAME_HINT_EXCLUDE = re.compile(r"bakeri|konditori")
NAME_HINT_WORDS = {
    "trans", "trp", "frakt", "rahti", "cargo", "gods", "bud", "express",
    "expressen", "xpress", "trucking", "truck", "trailer", "container",
    "shipping", "freight", "forwarding", "flytt", "muutto", "logistics",
    "logistik", "logistikk", "akeri", "aakeri", "bring", "hauling",
}


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

# Wall-clock budget for the whole discovery phase. The CI step allows 25 min and
# the re-check + writes that follow need only seconds, so this leaves generous
# headroom. See run_discovery() for why a cap exists at all.
DISCOVERY_BUDGET_S = 15 * 60

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


def is_haulier_name(name: str) -> bool:
    """True if a company NAME reads like a transport business, regardless of the
    industry code it is registered under. See the NAME_HINT_* comment above for
    why this is deliberately generous — DNS, not this function, decides who is
    an actual customer."""
    words = _ascii_words(name)
    if not words:
        return False
    if NAME_HINT_WORDS & set(words):
        return True
    joined = "".join(words)
    if NAME_HINT_EXCLUDE.search(joined):
        return False
    return any(stem in joined for stem in NAME_HINT_STEMS)


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
    must never be counted as a lost customer.

    This is the careful path, used for the daily RE-CHECK of hosts we already
    know: there, mistaking a blip for an NXDOMAIN invents a churn event, so the
    retries earn their cost over a few hundred hosts. Candidate sweeps use
    probe_host() instead — see why there."""
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


def probe_host(host: str) -> tuple[str, Optional[str], str]:
    """Single-shot resolve, for DISCOVERY candidates only. Deliberately does NOT
    retry: a candidate sweep is ~99 % misses, and resolve_host()'s 3 attempts +
    0.3 s sleeps cost 3 queries and 0.6 s of sleep on every one of them. That is
    what made discovery unable to finish — measured 2026-09-30, one attempt does
    ~1570 hosts/s against ~65 hosts/s for the retrying version, a 24x gap, which
    is the difference between a 6-minute register sweep and a 77-minute one.

    The tradeoff is safe in this direction: a real tenant lost to a one-off DNS
    blip is simply found by the next day's sweep, and once it is known it moves
    onto the resolve_host() path where a false 'dead' would actually matter."""
    try:
        return host, socket.gethostbyname(host), "live"
    except Exception:
        return host, None, "dead"


def probe_many(hosts: Iterable[str], workers: int = 64) -> set[str]:
    """Hosts that resolve, out of a large candidate list. Discovery-side only."""
    hosts = list(dict.fromkeys(hosts))
    if not hosts:
        return set()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return {h for h, ip, st in ex.map(probe_host, hosts) if st == "live"}


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


# ── discovery: Wayback Machine CDX index ────────────────────────────────────
def discover_wayback() -> set[str]:
    """The Internet Archive's CDX index is a public, wildcard-queryable list of
    every URL it has ever archived, so it answers "which *.opter.cloud hosts have
    ever existed?" directly. Tenants land in it because the archive follows links
    and because anyone can submit a page by hand — a typical row is
    https://malmoflygfraktse.opter.cloud/Account/Login.

    Highest-yield single source we have: it reaches tenants the passive-DNS
    aggregators never indexed, and countries register-guessing cannot cover at
    all (Denmark's register is unwired, Estonia's absent). Measured 2026-09-30:
    126 distinct hosts, 20 of them live tenants missing from the then-288
    universe, including 2 DK and 2 EE.

    CDX answers "Temporarily Offline" (HTTP 503) fairly often, so a couple of
    short retries. It also returns long-dead hosts; those just fail the DNS
    re-check and are recorded as known-dead instead of counted.
    """
    url = (f"https://web.archive.org/cdx/search/cdx?url=*.{BASE_DOMAIN}"
           "&fl=original&collapse=urlkey&limit=50000")
    rx = re.compile(r"https?://([a-z0-9][a-z0-9-]*)" + re.escape("." + BASE_DOMAIN))
    text = ""
    for attempt in range(3):
        r = SESSION.get(url, timeout=120)
        if r.status_code == 200:
            text = r.text.lower()
            break
        print(f"  wayback: HTTP {r.status_code} (attempt {attempt + 1}/3)")
        time.sleep(3)
    else:
        print("  wayback: index unavailable — skipped")
        return set()
    slugs = {m.group(1) for line in text.splitlines() if (m := rx.match(line.strip()))}
    print(f"  wayback: {len(slugs)} distinct hosts in the archive index")
    return {f"{s}.{BASE_DOMAIN}" for s in slugs}


# ── discovery: register guessing ────────────────────────────────────────────
def _guess_from_names(names: Iterable[str], suffix: str, label: str) -> set[str]:
    cands: set[str] = set()
    for nm in names:
        cands |= slug_candidates(nm)
    hosts = slugs_to_hosts(cands, suffix)
    t0 = time.time()
    hits = probe_many(hosts, workers=64)
    print(f"  register[{label}]: {len(cands)} slug candidates → {len(hits)} live "
          f"({time.time() - t0:.0f}s)")
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
    n_code = n_name = 0
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        fn = [n for n in z.namelist() if n.endswith(".txt")][0]
        import csv
        with z.open(fn) as fh:
            text = io.TextIOWrapper(fh, encoding="iso-8859-1", newline="")
            for row in csv.DictReader(text, delimiter="\t", quoting=csv.QUOTE_NONE):
                if row.get("FtgStat") != "1":  # only active
                    continue
                row_names = [row[k] for k in ("Namn", "Foretagsnamn") if row.get(k)]
                if not row_names:
                    continue
                if {row.get(f"Ng{i}") for i in range(1, 6)} & NACE_PLAIN:
                    names.extend(row_names)
                    n_code += 1
                elif any(is_haulier_name(n) for n in row_names):
                    names.extend(row_names)
                    n_name += 1
    print(f"  register[SE]: {n_code} firms on a transport code + {n_name} more "
          f"whose name reads as transport ({len(names)} names)")
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
    n_code = n_name = 0
    for e in data:
        if e.get("konkurs") or e.get("underAvvikling") or e.get("slettedato"):
            continue
        name = e.get("navn")
        if not name:
            continue
        codes = {e.get(k, {}).get("kode") for k in
                 ("naeringskode1", "naeringskode2", "naeringskode3") if e.get(k)}
        if codes & NACE_DOTTED:
            names.append(name)
            n_code += 1
        elif is_haulier_name(name):
            names.append(name)
            n_name += 1
    print(f"  register[NO]: {n_code} firms on a transport code + {n_name} more "
          f"whose name reads as transport")
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
    n_code = n_name = 0
    for c in companies:
        # In this bulk file the NACE code lives in mainBusinessLine.type
        # (e.g. "49410"), NOT ".code" — confirmed against data_YYYYMMDD.json.
        mbl = c.get("mainBusinessLine")
        code = mbl.get("type") if isinstance(mbl, dict) else None
        current = [n["name"] for n in c.get("names", [])
                   if isinstance(n, dict) and not n.get("endDate") and n.get("name")]
        if not current:
            continue
        if code in NACE_PLAIN:
            names.extend(current)
            n_code += 1
        elif any(is_haulier_name(n) for n in current):
            names.extend(current)
            n_name += 1
    print(f"  register[FI]: {n_code} firms on a transport code + {n_name} more "
          f"whose name reads as transport ({len(names)} names)")
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
                    sorted(NACE_DK)}},
        }
        r = SESSION.post(endpoint, json=query, auth=(user, pw), timeout=60)
        r.raise_for_status()
        print("  register[DK]: credentials present — implement paging in "
              "discover_denmark() to harvest names")
    except Exception as e:
        print(f"  register[DK]: skipped ({type(e).__name__}: {e})")
    return set()


# Ordered cheapest-first, because run_discovery() stops starting new sources once
# DISCOVERY_BUDGET_S is spent. The three bulk registers are the expensive ones
# (each is a 50-200 MB download plus a 50-120k-host DNS sweep; measured
# 2026-09-30 at 144 s / 581 s / 216 s for SE / NO / FI), so anything cheap must
# come before them or it never gets a turn — Denmark sat last and was starved.
DISCOVERY_SOURCES = [
    ("passive_dns", discover_passive_dns),   # ~6 s
    ("wayback_cdx", discover_wayback),       # ~14 s
    ("register_dk", discover_denmark),       # one API query (inert without creds)
    ("register_se", discover_sweden),
    ("register_no", discover_norway),
    ("register_fi", discover_finland),
]


def run_discovery(budget_s: float = DISCOVERY_BUDGET_S) -> dict[str, str]:
    """Run every discovery source, best-effort. Returns {host: source}. A source
    that raises or overruns is logged and skipped; discovery failing entirely is
    fine — the re-check job below still runs.

    Sources are attempted in order until `budget_s` of wall clock is used up,
    then the rest are skipped for the day. This is the guard that stops discovery
    from costing us the run: the CI step is capped at 25 minutes, and if the step
    is killed mid-discovery then the state file, Excel and DB writes at the end of
    main() never happen at all — which is exactly what silently happened every
    night from 2026-09-19 to 2026-09-30. The cheapest sources are listed first,
    so a bad day still gets the passive/archive indexes."""
    discovered: dict[str, str] = {}
    started = time.time()
    for label, fn in DISCOVERY_SOURCES:
        left = budget_s - (time.time() - started)
        if left <= 0:
            print(f"  discovery[{label}]: skipped (budget of {budget_s:.0f}s spent; "
                  f"runs next time)")
            continue
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
