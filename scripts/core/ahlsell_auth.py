"""
core/ahlsell_auth.py

Logs in to ahlsell.se and returns a requests.Session carrying the authenticated
cookies.

Why: the anonymous API exposes only per-branch stock. Central-warehouse stock
(`globalStock`), inbound quantities and prices come from
/api/product/{variantNumber}/info, which answers 204 No Content unless the
session is authenticated.

Mechanics (verified 2026-09-09): ahlsell.se uses a plain ASP.NET form POST --
GET /login/ sets .ASPXANONYMOUS / ASP.NET_SessionId and embeds a
__RequestVerificationToken, which must be posted back alongside the credentials.
There is no CAPTCHA, MFA or bot challenge. GET requests need no anti-forgery
header; only POST/PUT/PATCH do.

Credentials come from AHLSELL_USERNAME / AHLSELL_PASSWORD -- .env locally
(gitignored), repository secrets in CI. Never hardcode, commit, or print them.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(REPO_ROOT / ".env")

BASE_URL  = "https://www.ahlsell.se"
LOGIN_URL = f"{BASE_URL}/login"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "sv-SE,sv;q=0.9",
}

_TOKEN_RE = re.compile(
    r'<input[^>]*name="__RequestVerificationToken"[^>]*value="([^"]+)"'
)


class AhlsellAuthError(RuntimeError):
    """Login did not produce an authenticated session."""


def _extract_token(html: str) -> str:
    """Pulls the first __RequestVerificationToken out of the login page.

    The page carries two login forms (`login-form` -> /login and
    `loginPagePartialForm` -> /login/) with different tokens; either is
    accepted by its own endpoint, and we post to /login with the first.
    """
    m = _TOKEN_RE.search(html)
    if not m:
        raise AhlsellAuthError(
            "No __RequestVerificationToken on /login/ -- the login form has changed."
        )
    return m.group(1)


def is_authenticated(session: requests.Session, probe_variant: str = "1377701") -> bool:
    """True if this session can read authenticated product data.

    Uses the endpoint we actually care about rather than a cookie name: the
    anonymous session gets 204 No Content, an authenticated one gets 200 with
    a body. That makes this a capability check, not a guess.
    """
    try:
        r = session.get(
            f"{BASE_URL}/api/product/{probe_variant}/info",
            headers={**HEADERS, "Accept": "application/json", "Referer": f"{BASE_URL}/"},
            timeout=30,
        )
    except requests.RequestException:
        return False
    return r.status_code == 200 and bool(r.content)


def login(
    username: Optional[str] = None,
    password: Optional[str] = None,
    timeout: int = 30,
) -> requests.Session:
    """Returns an authenticated Session, or raises AhlsellAuthError.

    Callers that must not fail hard should use try_login() instead.
    """
    username = username or os.environ.get("AHLSELL_USERNAME")
    password = password or os.environ.get("AHLSELL_PASSWORD")
    if not username or not password:
        raise AhlsellAuthError(
            "AHLSELL_USERNAME / AHLSELL_PASSWORD not set. Locally: add them to "
            ".env. In CI: pass them as step env vars backed by repository secrets."
        )

    session = requests.Session()
    session.headers.update(HEADERS)

    page = session.get(f"{BASE_URL}/login/", timeout=timeout)
    page.raise_for_status()
    token = _extract_token(page.text)

    resp = session.post(
        LOGIN_URL,
        data={
            "__RequestVerificationToken": token,
            "username": username,
            "password": password,
        },
        headers={"Referer": f"{BASE_URL}/login/",
                 "Content-Type": "application/x-www-form-urlencoded"},
        timeout=timeout,
        allow_redirects=True,
    )

    if not is_authenticated(session):
        # Deliberately vague: never echo credentials or response bodies, which
        # can carry account identifiers.
        raise AhlsellAuthError(
            f"Login POST returned {resp.status_code} but the session is still "
            f"anonymous (/api/product/.../info gave no content). Check the "
            f"credentials, or the login form may have changed."
        )
    return session


def try_login(**kwargs) -> tuple[Optional[requests.Session], Optional[str]]:
    """Best-effort login. Returns (session, None) or (None, error_message).

    Never raises: a production script must still collect its anonymous
    branch-level data when the login fails, exactly as it did before auth
    existed.
    """
    try:
        return login(**kwargs), None
    except Exception as e:
        return None, str(e)
