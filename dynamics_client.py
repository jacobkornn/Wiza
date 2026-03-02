import os
import time
import math
import requests
import pandas as pd
import urllib.parse
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

load_dotenv()

# --- Constants ---
DYNAMICS_ORG_URL = os.getenv("DYNAMICS_ORG_URL")
DYNAMICS_API = f"{DYNAMICS_ORG_URL}/api/data/v9.2"

# --- Token management (singleton MSAL app, cached token with auto-refresh) ---
_msal_app = None
_cached_token = None
_token_acquired_at = 0.0

def _init_msal():
    global _msal_app
    if _msal_app is None:
        _msal_app = ConfidentialClientApplication(
            client_id=os.getenv("DYNAMICS_CLIENT_ID"),
            client_credential=os.getenv("DYNAMICS_CLIENT_SECRET"),
            authority=f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}"
        )
    return _msal_app

def get_token(force_refresh=False):
    global _cached_token, _token_acquired_at
    app = _init_msal()

    if not force_refresh and _cached_token and "access_token" in _cached_token:
        expires_in = int(_cached_token.get("expires_in", 0))
        if time.time() < _token_acquired_at + expires_in - 60:
            return _cached_token["access_token"]

    print("🔑 Acquiring Dynamics access token...")
    token = app.acquire_token_for_client(scopes=[f"{DYNAMICS_ORG_URL}/.default"])
    if "access_token" not in token:
        raise RuntimeError(f"Token request failed: {token}")
    _cached_token = token
    _token_acquired_at = time.time()
    print("✅ Token acquired successfully")
    return token["access_token"]

# --- Session (reuses TCP connections, auto-refreshes auth header) ---
_session = None

def get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
    _session.headers["Authorization"] = f"Bearer {get_token()}"
    return _session

# --- Shared utilities ---
def sanitize(value):
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, str):
        v = value.strip()
        return None if v.lower() == "nan" else v
    return value

def extract_domain(url_or_email):
    if not url_or_email:
        return None
    try:
        if pd.isna(url_or_email):
            return None
    except (TypeError, ValueError):
        pass
    s = str(url_or_email).strip().lower()
    if not s or s == "nan":
        return None
    if "@" in s:
        return s.split("@")[-1]
    if not s.startswith(("http://", "https://")):
        s = f"https://{s}"
    try:
        host = urllib.parse.urlparse(s).hostname
        if not host:
            return None
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return None
