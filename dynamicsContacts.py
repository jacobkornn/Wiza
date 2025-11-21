import os
import math
import shutil
import requests
import pandas as pd
import urllib.parse
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

load_dotenv()

# --- Dynamics config ---
DYNAMICS_ORG_URL = os.getenv("DYNAMICS_ORG_URL")
DYNAMICS_API = f"{DYNAMICS_ORG_URL}/api/data/v9.2"

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
    "Accept": "application/json"
}

# --- Utilities ---
def sanitize(value):
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, str):
        v = value.strip()
        return None if v.lower() == "nan" else v
    return value

def normalize_title(title):
    if not title:
        return None
    t = str(title).strip().lower()
    replacements = {
        "sr.": "senior", "sr": "senior",
        "jr.": "junior", "jr": "junior",
        "mgr": "manager", "dir": "director",
        "vp": "Vice President", "svp": "Senior Vice President",
        "evp": "Executive Vice President",
        "cto": "CTO", "cio": "CIO", "ciso": "CISO",
        "cfo": "CFO", "coo": "COO", "ceo": "CEO",
        "eng": "Engineer", "eng.": "Engineer",
    }

    words = [replacements.get(w, w) for w in t.split()]
    return " ".join([w.upper() if w in {"cto","cio","ciso","cfo","coo","ceo"} else w.capitalize() for w in words])

def classify_leadtype(list_name):
    if not list_name:
        return None
    ln = str(list_name).lower()
    if "engineering" in ln: return "Engineering"
    if "sales" in ln: return "Sales"
    if "it" in ln or "information technology" in ln: return "IT"
    return None

def normalize_headers(df):
    header_map = {
        "firstname": ["first name", "firstname", "first_name"],
        "lastname": ["last name", "lastname", "last_name"],
        "jobtitle": ["title","job title","job_title","jobtitle","job tittle","jobtittle","job ttile","joobtitle"],
        "accountname": ["company","company name","account","account name"],
        "emailaddress1": ["email","email address","emailaddress1"],
        "list_name": ["list_name","list name"],
        "websiteurl": ["website","website url","websiteurl"],
        "city": ["city"],
        "state": ["state","state/province"],
        "country": ["country"],
    }

    col_map = {}
    for col in df.columns:
        lc = col.strip().lower()
        for target, variants in header_map.items():
            if lc in variants:
                col_map[col] = target
                break

    return df.rename(columns=col_map)

def _norm_name(name):
    return " ".join(str(name).strip().lower().split()) if name else None

def _extract_domain(website_or_email):
    if not website_or_email:
        return None
    s = str(website_or_email).strip().lower()

    if "@" in s:
        return s.split("@")[-1]

    if not s.startswith(("http://", "https://")):
        s = f"https://{s}"

    try:
        host = urllib.parse.urlparse(s).hostname
        if not host:
            return None
        return host[4:] if host.startswith("www.") else host
    except:
        return None

def is_non_english(text):
    if not text:
        return False
    try:
        text.encode("ascii")
        return False
    except UnicodeEncodeError:
        return True


# --- Preload Dynamics data ---
def fetch_all_accounts():
    print("📥 Fetching all Accounts...")
    accounts, domains = {}, {}
    url = f"{DYNAMICS_API}/accounts?$select=accountid,name,websiteurl"

    while url:
        res = requests.get(url, headers=AUTH_HEADER)
        if not res.ok:
            raise RuntimeError(f"Accounts fetch failed: {res.status_code} {res.text}")
        data = res.json()

        for a in data.get("value", []):
            accid = a.get("accountid")
            name = sanitize(a.get("name"))
            web = sanitize(a.get("websiteurl"))

            if name and accid:
                accounts[_norm_name(name)] = accid

            dom = _extract_domain(web)
            if dom:
                domains[dom] = accid

        url = data.get("@odata.nextLink")

    print(f"✅ Loaded {len(accounts)} accounts; {len(domains)} domains")
    return accounts, domains


def fetch_all_contacts():
    print("📥 Fetching all Contacts...")
    contacts_by_email = {}
    contacts_by_fullname = {}

    url = f"{DYNAMICS_API}/contacts?$select=contactid,fullname,emailaddress1"
    while url:
        res = requests.get(url, headers=AUTH_HEADER)
        if not res.ok:
            raise RuntimeError(f"Contacts fetch failed: {res.status_code} {res.text}")

        data = res.json()
        for c in data.get("value", []):
            cid = c.get("contactid")

            email = sanitize(c.get("emailaddress1"))
            fullname = sanitize(c.get("fullname"))

            if email:
                contacts_by_email[email.lower()] = cid
            if fullname:
                contacts_by_fullname[fullname.lower()] = cid

        url = data.get("@odata.nextLink")

    print(f"✅ Loaded {len(contacts_by_email)} contacts by email, {len(contacts_by_fullname)} by fullname")
    return contacts_by_email, contacts_by_fullname


# --- Upsert helpers ---
def upsert_account(name, accounts_map, domains_map, extra=None):
    key = _norm_name(name)
    if key in accounts_map:
        return accounts_map[key]

    payload = {"name": sanitize(name)}
    if extra:
        for k, v in extra.items():
            sv = sanitize(v)
            if sv:
                payload[k] = sv

    res = requests.post(f"{DYNAMICS_API}/accounts", json=payload, headers=AUTH_HEADER)
    if not res.ok:
        raise RuntimeError(f"Account creation failed: {res.status_code} {res.text}")

    entity_id = res.headers.get("OData-EntityId")
    account_id = entity_id.split("(")[1].split(")")[0]

    accounts_map[key] = account_id

    dom = _extract_domain(extra.get("websiteurl")) if extra else None
    if dom:
        domains_map[dom] = account_id

    print(f"➕ Account created: {name} (ID={account_id})")
    return account_id


def upsert_contact(payload, email_map, fullname_map):
    email = sanitize(payload.get("emailaddress1"))
    fullname = sanitize(payload.get("fullname"))

    cid = None
    if email:
        cid = email_map.get(email.lower())
    if not cid and fullname:
        cid = fullname_map.get(fullname.lower())

    if cid:
        res = requests.patch(f"{DYNAMICS_API}/contacts({cid})", json=payload, headers=AUTH_HEADER)
        if not res.ok:
            raise RuntimeError(f"Contact update failed: {res.status_code} {res.text}")
        return cid

    # Create new
    res = requests.post(f"{DYNAMICS_API}/contacts", json=payload, headers=AUTH_HEADER)
    if not res.ok:
        raise RuntimeError(f"Contact creation failed: {res.status_code} {res.text}")

    entity_id = res.headers.get("OData-EntityId")
    contact_id = entity_id.split("(")[1].split(")")[0]

    if email:
        email_map[email.lower()] = contact_id
    if fullname:
        fullname_map[fullname.lower()] = contact_id

    print(f"➕ Contact created: {fullname or email} (ID={contact_id})")
    return contact_id


# --- Account resolver ---
def resolve_account_id(row, accounts_map, domains_map):
    company = sanitize(row.get("accountname"))
    website = sanitize(row.get("websiteurl"))
    email = sanitize(row.get("emailaddress1"))

    name_key = _norm_name(company)
    web_domain = _extract_domain(website)
    email_domain = _extract_domain(email)

    if name_key and name_key in accounts_map:
        return accounts_map[name_key]
    if web_domain and web_domain in domains_map:
        return domains_map[web_domain]
    if email_domain and email_domain in domains_map:
        return domains_map[email_domain]

    if company:
        extra_account = {
            "websiteurl": website,
            "address1_city": sanitize(row.get("city")),
            "address1_stateorprovince": sanitize(row.get("state")),
            "address1_country": sanitize(row.get("country")),
        }
        return upsert_account(company, accounts_map, domains_map, extra_account)

    return None


# --- File discovery ---
def discover_wiza_csvs():
    downloads = os.path.join(os.path.expanduser("~"), "Downloads")
    return [
        os.path.join(downloads, f)
        for f in os.listdir(downloads)
        if f.startswith("WIZA") and f.lower().endswith(".csv")
    ]


# --- Archive original file ---
def archive_original_file(src_path):
    digest_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Digest")
    os.makedirs(digest_dir, exist_ok=True)

    try:
        shutil.move(src_path, os.path.join(digest_dir, os.path.basename(src_path)))
        print(f"📦 Moved to Digest/: {os.path.basename(src_path)}")
    except Exception as e:
        print(f"❌ Move failed for {src_path}: {e}")


# --- Main ingestion ---
def ingest_wiza_file(file_path, accounts_map, domains_map, email_map, fullname_map):
    print(f"\n📄 Processing: {os.path.basename(file_path)}")

    df = pd.read_csv(file_path).astype(object).where(pd.notnull, None)
    df = normalize_headers(df)

    created = 0
    skipped = 0
    failures = 0

    for _, row in df.iterrows():
        try:
            firstname = sanitize(row.get("firstname"))
            lastname = sanitize(row.get("lastname"))
            email = sanitize(row.get("emailaddress1"))

            if is_non_english(firstname) or is_non_english(lastname):
                skipped += 1
                continue

            account_id = resolve_account_id(row, accounts_map, domains_map)
            if not account_id:
                skipped += 1
                continue

            fullname = f"{firstname or ''} {lastname or ''}".strip() or None

            payload = {
                "firstname": firstname,
                "lastname": lastname,
                "fullname": fullname,
                "jobtitle": normalize_title(sanitize(row.get("jobtitle"))),
                "emailaddress1": email,
                "cr21a_leadtype": classify_leadtype(sanitize(row.get("list_name"))),
            }

            payload = {k: v for k, v in payload.items() if v}

            if not payload.get("emailaddress1") and not payload.get("fullname"):
                skipped += 1
                continue

            cid = upsert_contact(payload, email_map, fullname_map)

            # Attach account
            ref = f"{DYNAMICS_API}/contacts({cid})/parentcustomerid_account/$ref"
            ref_payload = {"@odata.id": f"{DYNAMICS_API}/accounts({account_id})"}

            ref_res = requests.put(ref, json=ref_payload, headers=AUTH_HEADER)
            if not ref_res.ok:
                failures += 1
                print(f"❌ Link failed for contact {cid}: {ref_res.status_code} {ref_res.text}")
            else:
                print(f"✅ Linked contact {cid} → account {account_id}")

            created += 1

        except Exception as e:
            failures += 1
            print(f"❌ Row failed: {e}")

    print(
        f"📊 Summary {os.path.basename(file_path)} → "
        f"{created} created, {skipped} skipped, {failures} failed"
    )


# --- Main ---
def main():
    files = discover_wiza_csvs()
    if not files:
        print("ℹ️ No WIZA CSV files found.")
        return

    accounts_map, domains_map = fetch_all_accounts()
    email_map, fullname_map = fetch_all_contacts()

    for fp in files:
        ingest_wiza_file(fp, accounts_map, domains_map, email_map, fullname_map)
        archive_original_file(fp)

    print("\n✅ Wiza ingestion complete.")


if __name__ == "__main__":
    main()
