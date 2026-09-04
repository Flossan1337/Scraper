"""
core/trends.py

Shared DB-write helper for the 8 Google Trends fetchers, all of which
produce the same shape (a wide DataFrame: a "Date" column plus one column
per search term/geo series) and share one table, google_trends_monthly.
See sql/schema.sql for why this is upserted (latest-known-state) rather
than snapshot-inserted.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from core.db import safe_upsert


def write_trends_to_db(
    pipeline: str,
    sheets: dict[str, pd.DataFrame],
) -> tuple[int | None, str | None]:
    """
    Best-effort: upsert one script's full monthly trends output.

    sheets: {sheet_label: df}, where df has a "Date" column plus one column
    per series. Use {"default": df} for a script with a single sheet.
    """
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for sheet_label, df in sheets.items():
        if df is None or df.empty:
            continue
        series_cols = [c for c in df.columns if c != "Date"]
        for _, row in df.iterrows():
            date_val = row["Date"]
            month = date_val.date() if hasattr(date_val, "date") else date_val
            for series in series_cols:
                value = row[series]
                if pd.isna(value):
                    continue
                rows.append((pipeline, sheet_label, series, month, float(value), fetched_at))

    if not rows:
        return 0, None

    return safe_upsert(
        table="google_trends_monthly",
        columns=["pipeline", "sheet", "series", "month", "value", "fetched_at"],
        rows=rows,
        conflict_columns=["pipeline", "sheet", "series", "month"],
    )


# ---------------------------------------------------------------------------
# TrendReq - drop-in replacement for pytrends.request.TrendReq
# ---------------------------------------------------------------------------
# Why this exists (measured 2026-09-04, not guessed):
#
# Google Trends answers 429 to the FIRST request of a fresh HTTP session and
# uses that 429 response to set the NID cookie. Every subsequent request on
# the SAME session succeeds. Measured: 40 back-to-back explore calls with no
# delay at all -> 39x 200, and the single 429 was request #0. Headers are
# irrelevant; a bare session with no User-Agent behaves identically.
#
# pytrends fails today because it does the exact opposite: a new TrendReq per
# country and per retry means every request is a session's first request, so
# every request gets a 429. The old scripts then read that as "rate limited",
# slept 60s+ with exponential backoff, and built ANOTHER fresh client - which
# guaranteed another 429. The backoff was causing the failure, not surviving
# it. That is why those runs took ages and still ended with no data.
#
# So: one process-wide warmed session, a cached cookie on disk, and short
# retries measured in seconds. Do NOT "fix" a 429 here by adding long sleeps
# or by building a fresh session per request - that is the bug, not the cure.
#
# The public surface mirrors the slice of pytrends these scripts use:
#   TrendReq(...).build_payload(kw_list, cat, timeframe, geo, gprop)
#   .interest_over_time() -> DataFrame indexed by "date", one column per
#                            keyword (int), plus a bool "isPartial" column.
# Legacy pytrends kwargs (retries, backoff_factor, requests_args, ...) are
# accepted and ignored so callers need only swap the import.

import json as _json
import random as _random
import time as _time
from pathlib import Path as _Path

import requests as _requests

_EXPLORE_URL = "https://trends.google.com/trends/api/explore"
_MULTILINE_URL = "https://trends.google.com/trends/api/widgetdata/multiline"

# Cookie cache: skips the warmup 429 on reruns. Derived, disposable, gitignored.
_COOKIE_CACHE = _Path(__file__).resolve().parent.parent.parent / ".cache" / "trends_cookie.json"

# Sporadic 429s still happen on a warmed session, and a short retry clears
# them. These are SECONDS, deliberately - see the note above fetch_series()
# about why long backoff cannot help against the per-IP quota.
_RETRY_SLEEPS = [1.0, 3.0]

# Once the per-IP quota trips, every request 429s until it clears. Continuing
# to try wastes minutes per script and spends quota that is already gone, so
# after this many fully-failed requests in a row we stop the run immediately.
_QUOTA_TRIP_AFTER = 3

_SESSION = None
_CONSECUTIVE_FAILURES = 0

# The cold-start cookie wait below is worth ~1 min ONCE. Paying it again on
# every session rebuild would turn a quota-blocked run into a 6-minute grind
# instead of the ~20s fail-fast the breaker is there to give, so both the wait
# and the rebuild-on-429 are capped at one per process.
_AGED_ONCE = False
_RESET_ONCE = False


class TrendsError(RuntimeError):
    """A Trends request that did not recover after retries."""


class TrendsQuotaError(TrendsError):
    """The per-IP quota is exhausted; nothing will succeed until it clears."""


def _load_cached_cookie():
    try:
        return _json.loads(_COOKIE_CACHE.read_text()).get("NID") or None
    except Exception:
        return None


def _save_cached_cookie(nid: str) -> None:
    try:
        _COOKIE_CACHE.parent.mkdir(parents=True, exist_ok=True)
        _COOKIE_CACHE.write_text(_json.dumps({"NID": nid}))
    except Exception:
        pass  # cache is an optimisation; never fail a run over it


def _new_session():
    """A session holding a valid NID cookie, from cache or from a warmup call."""
    s = _requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )

    cached = _load_cached_cookie()
    if cached:
        # An already-aged cookie works immediately - this is why the cache is
        # worth keeping: only the very first run ever pays the handshake wait.
        s.cookies.set("NID", cached, domain=".google.com")
        return s

    # Cold start. Google answers the first request of a new session with a 429
    # and sets NID on that response - the 429 IS the handshake, not an error.
    #
    # The cookie then has to AGE before Google accepts it. Measured 2026-09-04:
    # the same session got 429 at +5s, +15s and +30s after the handshake, then
    # 200 at +60s, and an immediate burst of 5 more calls all returned 200. So
    # this wait is once per cold start, not per request - and the cookie we
    # save means the next run skips it entirely.
    probe = {
        "comparisonItem": [{"keyword": "google", "geo": "", "time": "today 12-m"}],
        "category": 0,
        "property": "",
    }
    params = {
        "hl": "en-US",
        "tz": "-120",
        "req": _json.dumps(probe, separators=(",", ":")),
    }

    def _try():
        try:
            return s.get(_EXPLORE_URL, params=params, timeout=(10, 30)).status_code
        except _requests.RequestException:
            return None

    global _AGED_ONCE

    _try()  # handshake: sets NID, expected to be a 429
    if not s.cookies.get("NID") or _AGED_ONCE:
        # No cookie, or we already spent the wait once in this process - let
        # the real call and the quota breaker decide what happens next.
        return s

    # Without this the script looks hung for a minute on a cold start.
    print("Google Trends: ny session, vantar in cookien (~1 min, en gang)...", flush=True)

    # Poll until the cookie goes live. Budget ~2 min, then hand back anyway so
    # the caller's own error path (and the quota breaker) takes over.
    for delay in [15.0, 15.0, 15.0, 15.0, 20.0, 20.0, 20.0]:
        _time.sleep(delay)
        if _try() == 200:
            _AGED_ONCE = True
            _save_cached_cookie(s.cookies.get("NID"))
            return s

    _AGED_ONCE = True
    return s


def _session(reset: bool = False):
    global _SESSION
    if reset:
        _SESSION = None
        try:
            _COOKIE_CACHE.unlink()
        except OSError:
            pass
    if _SESSION is None:
        _SESSION = _new_session()
    return _SESSION


def _get_json(url: str, params: dict) -> dict:
    """GET a Trends JSON endpoint, retrying briefly through sporadic 429s."""
    global _CONSECUTIVE_FAILURES, _RESET_ONCE

    if _CONSECUTIVE_FAILURES >= _QUOTA_TRIP_AFTER:
        raise TrendsQuotaError(
            "Google Trends per-IP quota exhausted ({} requests failed in a row). "
            "Nothing will succeed until it clears - wait and rerun; cached "
            "series are kept, so the rerun only fetches what is missing.".format(
                _CONSECUTIVE_FAILURES
            )
        )

    last = "no attempt made"
    for attempt, delay in enumerate([0.0] + _RETRY_SLEEPS):
        if delay:
            # jitter so repeated retries do not resynchronise
            _time.sleep(delay + _random.uniform(0, 0.5))
        try:
            resp = _session().get(url, params=params, timeout=(10, 30))
        except _requests.RequestException as exc:
            last = "{}: {}".format(type(exc).__name__, exc)
            continue

        if resp.status_code == 200:
            body = resp.text
            brace = body.find("{")
            if brace == -1:
                last = "200 but no JSON body"
                continue
            try:
                parsed = _json.loads(body[brace:])
            except ValueError as exc:
                last = "unparseable JSON: {}".format(exc)
                continue
            _CONSECUTIVE_FAILURES = 0
            return parsed

        last = "HTTP {}".format(resp.status_code)
        # A stale/expired cookie 429s every time, so re-handshake - but only
        # once per process. If the quota is what is blocking us, rebuilding
        # again cannot help and just delays the breaker.
        if resp.status_code == 429 and attempt == 1 and not _RESET_ONCE:
            _RESET_ONCE = True
            _session(reset=True)

    _CONSECUTIVE_FAILURES += 1
    raise TrendsError(
        "{} failed after {} attempts ({})".format(url, len(_RETRY_SLEEPS) + 1, last)
    )


class TrendReq:
    """pytrends-compatible client backed by one warmed, reused session."""

    def __init__(self, hl="en-US", tz=360, geo="", timeout=(10, 30), **_legacy):
        # **_legacy swallows retries/backoff_factor/requests_args/proxies etc.
        self.hl = hl
        self.tz = tz
        self.geo = geo
        self.timeout = timeout
        self.kw_list = []
        self.interest_over_time_widget = None
        _session()  # warm up now, so the handshake cost is paid once

    def build_payload(self, kw_list, cat=0, timeframe="today 5-y", geo="", gprop=""):
        """Resolve the TIMESERIES widget + token for these keywords."""
        self.kw_list = list(kw_list)
        geo = geo or self.geo
        geos = geo if isinstance(geo, list) else [geo] * len(self.kw_list)
        times = timeframe if isinstance(timeframe, list) else [timeframe] * len(self.kw_list)

        req = {
            "comparisonItem": [
                {"keyword": kw, "geo": g, "time": t}
                for kw, g, t in zip(self.kw_list, geos, times)
            ],
            "category": cat,
            "property": gprop,
        }
        data = _get_json(
            _EXPLORE_URL,
            {
                "hl": self.hl,
                "tz": str(self.tz),
                "req": _json.dumps(req, separators=(",", ":")),
            },
        )
        widgets = [w for w in data.get("widgets", []) if w.get("id") == "TIMESERIES"]
        if not widgets:
            raise TrendsError(
                "No TIMESERIES widget returned for {} (geo={!r})".format(self.kw_list, geo)
            )
        self.interest_over_time_widget = widgets[0]

    def interest_over_time(self) -> pd.DataFrame:
        """Interest over time, shaped exactly like pytrends returns it."""
        if self.interest_over_time_widget is None:
            raise TrendsError("build_payload() must be called before interest_over_time()")

        w = self.interest_over_time_widget
        data = _get_json(
            _MULTILINE_URL,
            {
                "hl": self.hl,
                "tz": str(self.tz),
                "req": _json.dumps(w["request"], separators=(",", ":")),
                "token": w["token"],
            },
        )
        points = data.get("default", {}).get("timelineData", [])
        if not points:
            return pd.DataFrame()

        index = pd.to_datetime([float(p["time"]) for p in points], unit="s")
        out = pd.DataFrame(index=index)
        out.index.name = "date"
        for i, kw in enumerate(self.kw_list):
            out[kw] = [int(p["value"][i]) for p in points]
        out["isPartial"] = [bool(p.get("isPartial", False)) for p in points]
        return out.sort_index()


# ---------------------------------------------------------------------------
# fetch_series - cached, resumable single-series fetch
# ---------------------------------------------------------------------------
# Google enforces a real per-IP request quota on top of the per-session
# handshake described above. Once it trips, EVERY request 429s regardless of
# session or headers until it clears - retrying harder cannot help, it only
# spends more of the quota you no longer have.
#
# The defence is to spend fewer requests, not to sleep between them:
#
#   * one warmed session per process (see TrendReq above),
#   * every fetched series cached on disk under .cache/trends/,
#   * the cache keyed on the exact request, so a rerun on the same day is
#     free and a run that died halfway only refetches what is still missing.
#
# That last point is what makes a partially rate-limited run recoverable: run
# the script again and it converges instead of starting from zero.

import hashlib as _hashlib
from io import StringIO as _StringIO

_SERIES_CACHE_DIR = _COOKIE_CACHE.parent / "trends"


def _cache_key(kw_list, timeframe: str, geo: str, cat: int, gprop: str) -> str:
    raw = _json.dumps([list(kw_list), timeframe, geo, cat, gprop], sort_keys=True)
    return _hashlib.sha256(raw.encode()).hexdigest()[:20]


def clear_series_cache() -> None:
    """Drop every cached series (use when you want a guaranteed fresh pull)."""
    import shutil

    shutil.rmtree(_SERIES_CACHE_DIR, ignore_errors=True)


def fetch_series(
    kw_list,
    timeframe: str,
    geo: str = "",
    cat: int = 0,
    gprop: str = "",
    client: "TrendReq | None" = None,
    use_cache: bool = True,
) -> pd.DataFrame:
    """
    One interest-over-time series, served from disk when already fetched.

    Returns the same frame pytrends would: indexed by "date", one int column
    per keyword, plus a bool "isPartial". Raises TrendsError if the fetch
    fails and nothing is cached.
    """
    key = _cache_key(kw_list, timeframe, geo, cat, gprop)
    path = _SERIES_CACHE_DIR / (key + ".json")

    if use_cache and path.exists():
        try:
            payload = _json.loads(path.read_text())
            # io.StringIO, not the bare string: pandas deprecated literal-JSON
            # input to read_json.
            frame = pd.read_json(_StringIO(payload["frame"]), orient="split")
            frame.index = pd.to_datetime(frame.index)
            frame.index.name = "date"
            return frame
        except Exception:
            pass  # unreadable cache entry is just a miss

    client = client or TrendReq(hl="en-US", tz=120)
    client.build_payload(kw_list, cat=cat, timeframe=timeframe, geo=geo, gprop=gprop)
    frame = client.interest_over_time()
    if frame.empty:
        raise TrendsError(
            "Empty series for {} (geo={!r}, {})".format(list(kw_list), geo, timeframe)
        )

    try:
        _SERIES_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(
            _json.dumps(
                {
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "kw_list": list(kw_list),
                    "timeframe": timeframe,
                    "geo": geo,
                    "frame": frame.to_json(orient="split"),
                }
            )
        )
    except Exception:
        pass  # caching is an optimisation, never a reason to fail

    return frame
