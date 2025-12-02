import os
import math
import re
import requests
import pandas as pd
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

load_dotenv()

# --- Dynamics config --- #
DYNAMICS_ORG_URL = os.getenv("DYNAMICS_ORG_URL")
DYNAMICS_API = f"{DYNAMICS_ORG_URL}/api/data/v9.2"
ACCOUNTS_ENDPOINT = f"{DYNAMICS_API}/accounts"

def get_dynamics_token():
    app = ConfidentialClientApplication(
        client_id=os.getenv("DYNAMICS_CLIENT_ID"),
        client_credential=os.getenv("DYNAMICS_CLIENT_SECRET"),
        authority=f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}"
    )
    token = app.acquire_token_for_client(scopes=[f"{DYNAMICS_ORG_URL}/.default"])
    if "access_token" not in token:
        raise RuntimeError(f"Token request failed: {token}")
    return token["access_token"]

ACCESS_TOKEN = get_dynamics_token()
AUTH_HEADER = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "OData-MaxVersion": "4.0",
    "OData-Version": "4.0",
}

# --- Local Wiza CSV path --- #
WIZA_CSV_PATH = "WIZA_accounts-enriched_company_ID4517314 (1).csv"

# Optional: set to True to see what WOULD be updated without patching.
DRY_RUN = False


# ---------- Helpers ---------- #

def normalize_name(name: str) -> str:
    """
    Aggressively normalize account/company names for matching.
    """
    if not isinstance(name, str):
        return ""

    s = name.strip().lower().replace("\u00a0", " ")

    # Normalize & -> and
    s = s.replace("&", " and ")

    # Remove basic punctuation we don't care about
    s = re.sub(r"[.,'’]", "", s)

    # Collapse multiple spaces
    s = re.sub(r"\s+", " ", s).strip()

    # Strip common trailing regional decorations like "- US", "- USA", "- North America"
    s = re.sub(r"\s*[-–—]\s*(us|usa|u\.s\.a\.|north america|na)$", "", s).strip()

    # Remove common legal suffixes at the end, repeatedly
    suffixes = [
        " inc", " inc.", " incorporated",
        " llc", " llc.",
        " ltd", " ltd.", " limited",
        " co", " co.", " company",
        " corp", " corp.", " corporation",
        " plc",
        " gmbh",
        " s.a.", " sa"
    ]

    changed = True
    while changed and s:
        changed = False
        for suf in suffixes:
            if s.endswith(suf):
                s = s[: -len(suf)].rstrip()
                changed = True

    # Final whitespace collapse + strip
    s = re.sub(r"\s+", " ", s).strip()

    return s


def safe_val(v):
    """Turn NaN / empty string into None so we can skip it in the payload."""
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


def build_update_payload(wiza_row: pd.Series) -> dict:
    """
    Map Wiza columns onto Dynamics account fields.
    Only include fields that actually have values.
    """
    payload = {}

    # --- Domain + Website --- #
    domain_raw = safe_val(wiza_row.get("company_domain"))
    if isinstance(domain_raw, str) and domain_raw:
        domain_clean = domain_raw.strip()
        # Custom field: raw domain from Wiza
        payload["cr21a_domain"] = domain_clean

        # Websiteurl: https:// + domain if needed
        website = domain_clean
        if not (website.startswith("http://") or website.startswith("https://")):
            website = "https://" + website
        payload["websiteurl"] = website

    # --- Description --- #
    desc = safe_val(wiza_row.get("company_description"))
    if desc:
        payload["description"] = desc

    # --- Address fields --- #
    street = safe_val(wiza_row.get("company_street"))
    if street:
        payload["address1_line1"] = street

    city = safe_val(wiza_row.get("company_locality"))
    if city:
        payload["address1_city"] = city

    region = safe_val(wiza_row.get("company_region"))
    if region:
        payload["address1_stateorprovince"] = region

    country = safe_val(wiza_row.get("company_country"))
    if country:
        payload["address1_country"] = country

    postal = safe_val(wiza_row.get("company_postal_code"))
    if postal:
        payload["address1_postalcode"] = postal

    # --- Optional: custom fields (fill in your logical names if you want) --- #
    # industry = safe_val(wiza_row.get("company_industry"))
    # if industry:
    #     payload["cr21a_industry_text"] = industry
    #
    # size_range = safe_val(wiza_row.get("company_size_range"))
    # if size_range:
    #     payload["cr21a_companysizerange"] = size_range
    #
    # revenue = safe_val(wiza_row.get("company_revenue"))
    # if revenue:
    #     payload["cr21a_companyrevenue"] = revenue
    #
    # funding = safe_val(wiza_row.get("company_funding"))
    # if funding:
    #     payload["cr21a_companyfunding"] = funding
    #
    # linkedin = safe_val(wiza_row.get("company_linkedin"))
    # if linkedin:
    #     payload["cr21a_companylinkedin"] = linkedin
    #
    # profile_url = safe_val(wiza_row.get("profile_url"))
    # if profile_url:
    #     payload["cr21a_wizaprofileurl"] = profile_url

    return payload


def patch_account(account_id: str, payload: dict) -> bool:
    """
    PATCH a single account in Dynamics.
    Returns True on success, False on failure.
    """
    if not payload:
        print(f"   ⚪ Nothing to update for account {account_id} (empty payload)")
        return True

    url = f"{ACCOUNTS_ENDPOINT}({account_id})"
    headers = {**AUTH_HEADER, "If-Match": "*"}

    if DRY_RUN:
        print(f"   [DRY RUN] Would PATCH {url} with: {payload}")
        return True

    resp = requests.patch(url, headers=headers, json=payload)
    if 200 <= resp.status_code < 300:
        print(f"   ✅ PATCH success for {account_id}")
        return True
    else:
        print(f"   ❌ PATCH failed for {account_id}: {resp.status_code} {resp.text}")
        return False


def fetch_accounts_page(next_link: str | None = None):
    """
    Get a page of accounts from Dynamics.
    We only need accountid + name, because everything else will come from Wiza.
    Returns (records, next_link).
    """
    if next_link:
        url = next_link
    else:
        url = f"{ACCOUNTS_ENDPOINT}?$select=accountid,name&$top=5000"

    resp = requests.get(url, headers=AUTH_HEADER)
    resp.raise_for_status()
    data = resp.json()

    records = data.get("value", [])
    next_link = data.get("@odata.nextLink")
    return records, next_link


def build_wiza_name_index(csv_path: str):
    """
    Load Wiza CSV and build:
      name_index: normalized company name -> row
    """
    wiza_df = pd.read_csv(csv_path)

    name_index = {}
    for _, row in wiza_df.iterrows():
        raw = row.get("company")
        key = normalize_name(raw)
        if key and key not in name_index:
            name_index[key] = row

    print(f"📂 Loaded {len(wiza_df)} Wiza rows")
    print(f"🔑 Name index size (unique normalized names): {len(name_index)}")
    return name_index


def find_wiza_match_by_name(account: dict, name_index: dict) -> pd.Series | None:
    """
    Match Dynamics account to Wiza row by normalized name.
    """
    raw = account.get("name")
    key = normalize_name(raw)
    if not key:
        return None
    return name_index.get(key)


# ---------- Main enrichment logic ---------- #

def main():
    name_index = build_wiza_name_index(WIZA_CSV_PATH)

    total_accounts = 0
    matched_accounts = 0
    updated_accounts = 0
    skipped_no_match = 0

    next_link = None

    print("\n🔄 Starting Dynamics account enrichment from Wiza (name-only, cleaned matching)...\n")

    while True:
        accounts, next_link = fetch_accounts_page(next_link)
        if not accounts:
            break

        for acc in accounts:
            total_accounts += 1
            account_id = acc.get("accountid")
            account_name = acc.get("name")

            norm_name = normalize_name(account_name)
            print(f"👉 Processing account: '{account_name}' ({account_id}) | normalized: '{norm_name}'")

            wiza_row = find_wiza_match_by_name(acc, name_index)
            if wiza_row is None:
                skipped_no_match += 1
                print("   ⚪ No Wiza match by normalized name. Skipping.")
                continue

            matched_accounts += 1
            wiza_company_name = wiza_row.get("company")
            print(f"   🔗 Match found: Dynamics '{account_name}' ↔ Wiza '{wiza_company_name}'")

            payload = build_update_payload(wiza_row)
            if payload:
                print(f"   🧩 Enriching with fields: {', '.join(payload.keys())}")
            else:
                print("   ⚪ Wiza row has no usable enrichment fields (empty payload).")

            if patch_account(account_id, payload):
                updated_accounts += 1

        if not next_link:
            break

    print("\n------ SUMMARY ------")
    print(f"Total Dynamics accounts processed: {total_accounts}")
    print(f"Accounts with a Wiza name match:  {matched_accounts}")
    print(f"Accounts updated in Dynamics:     {updated_accounts}")
    print(f"Accounts with no Wiza match:      {skipped_no_match}")
    if DRY_RUN:
        print("\nDRY_RUN is ON – no changes were actually written to Dynamics.")


if __name__ == "__main__":
    main()
