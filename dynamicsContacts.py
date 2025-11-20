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
    print("🔑 Acquiring Dynamics access token...")
    app = ConfidentialClientApplication(
        client_id=os.getenv("DYNAMICS_CLIENT_ID"),
        client_credential=os.getenv("DYNAMICS_CLIENT_SECRET"),
        authority=f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}"
    )
    token = app.acquire_token_for_client(scopes=[f"{DYNAMICS_ORG_URL}/.default"])
    if "access_token" not in token:
        raise RuntimeError(f"Token request failed: {token}")
    print("✅ Token acquired")
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
    v = str(value).strip() if isinstance(value, str) else value
    if isinstance(v, str) and v.lower() == "nan":
        return None
    return v

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
        "eng": "Engineer", "eng.": "Engineer"
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
    col_map = {}
    for c in df.columns:
        lc = c.strip().lower()
        if lc in {"first name","firstname","first_name"}: col_map[c] = "firstname"
        elif lc in {"last name","lastname","last_name"}: col_map[c] = "lastname"
        elif lc in {"title","job title","job_title"}: col_map[c] = "jobtitle"
        elif lc in {"company","company name","account","account name"}: col_map[c] = "accountname"
        elif lc in {"email","email address","emailaddress1"}: col_map[c] = "emailaddress1"
        elif lc in {"list_name","list name"}: col_map[c] = "list_name"
        elif lc in {"website","website url"}: col_map[c] = "websiteurl"
        elif lc in {"city"}: col_map[c] = "city"
        elif lc in {"state","state/province"}: col_map[c] = "state"
        elif lc in {"country"}: col_map[c] = "country"
    return df.rename(columns=col_map)

def _norm_name(name):
    if not name: return None
    return " ".join(str(name).strip().lower().split())

def _extract_domain(website_or_email):
    if not website_or_email: return None
    s = str(website_or_email).strip()
    if "@" in s and "." in s:  # email
        return s.split("@")[-1].lower()
    try:
        if not s.startswith(("http://","https://")):
            s = f"https://{s}"
        parsed = urllib.parse.urlparse(s)
        host = (parsed.hostname or "").lower()
        if host.startswith("www."): host = host[4:]
        return host or None
    except Exception:
        return None

def is_non_english(text):
    if not text: return False
    try:
        text.encode("ascii")
        return False
    except UnicodeEncodeError:
        return True

# --- Dynamics preload ---
def fetch_all_accounts():
    print("📥 Fetching all Accounts...")
    accounts, domains = {}, {}
    url = f"{DYNAMICS_API}/accounts?$select=accountid,name,websiteurl"
    while url:
        res = requests.get(url, headers=AUTH_HEADER)
        if not res.ok: raise RuntimeError(f"Accounts fetch failed: {res.status_code} {res.text}")
        data = res.json()
        for a in data.get("value", []):
            accid = a.get("accountid")
            name = sanitize(a.get("name"))
            web = sanitize(a.get("websiteurl"))
            if name and accid: accounts[_norm_name(name)] = accid
            dom = _extract_domain(web)
            if dom and accid: domains[dom] = accid
        url = data.get("@odata.nextLink")
    print(f"✅ Loaded {len(accounts)} accounts; {len(domains)} domains")
    return accounts, domains

def fetch_all_contacts():
    print("📥 Fetching all Contacts...")
    contacts_by_email, contacts_by_fullname = {}, {}
    url = f"{DYNAMICS_API}/contacts?$select=contactid,fullname,emailaddress1"
    while url:
        res = requests.get(url, headers=AUTH_HEADER)
        if not res.ok: raise RuntimeError(f"Contacts fetch failed: {res.status_code} {res.text}")
        data = res.json()
        for c in data.get("value", []):
            cid = c.get("contactid")
            email = sanitize(c.get("emailaddress1"))
            fullname = sanitize(c.get("fullname"))
            if email: contacts_by_email[email.strip().lower()] = cid
            if fullname: contacts_by_fullname[fullname.strip().lower()] = cid
        url = data.get("@odata.nextLink")
    print(f"✅ Loaded {len(contacts_by_email)} contacts by email, {len(contacts_by_fullname)} by fullname")
    return contacts_by_email, contacts_by_fullname

# --- Upsert helpers ---
def upsert_account(name, accounts_map, domains_map, extra=None):
    if not name: return None
    key = _norm_name(name)
    if key in accounts_map: return accounts_map[key]
    payload = {"name": sanitize(name)}
    if extra:
        for k,v in extra.items():
            sv = sanitize(v)
            if sv: payload[k] = sv
    res = requests.post(f"{DYNAMICS_API}/accounts", json=payload, headers=AUTH_HEADER)
    if not res.ok: raise RuntimeError(f"Account creation failed: {res.status_code} {res.text}")
    entity_id = res.headers.get("OData-EntityId")
    account_id = entity_id.split("(")[1].split(")")[0]
    accounts_map[key] = account_id
    dom = _extract_domain(extra.get("websiteurl")) if extra else None
    if dom: domains_map[dom] = account_id
    print(f"➕ Account created: {name} (ID={account_id})")
    return account_id

def upsert_contact(contact_payload, contacts_by_email, contacts_by_fullname):
    email = sanitize(contact_payload.get("emailaddress1"))
    fullname = sanitize(contact_payload.get("fullname"))
    cid = None

    # Check if contact already exists
    if email:
        cid = contacts_by_email.get(email.strip().lower())
    if not cid and fullname:
        cid = contacts_by_fullname.get(fullname.strip().lower())

    if cid:
        print(f"⏭️ Contact exists: {fullname or email} (ID={cid})")
        return cid

    # Create new contact
    print(f"➡️ Creating contact {fullname or email} linked to account {contact_payload.get('parentcustomerid_account@odata.bind')}")
    res = requests.post(f"{DYNAMICS_API}/contacts", json=contact_payload, headers=AUTH_HEADER)
    if not res.ok:
        raise RuntimeError(f"Contact creation failed: {res.status_code} {res.text}")

    entity_id = res.headers.get("OData-EntityId")
    contact_id = entity_id.split("(")[1].split(")")[0]

    if email:
        contacts_by_email[email.strip().lower()] = contact_id
    if fullname:
        contacts_by_fullname[fullname.strip().lower()] = contact_id

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

    # Priority: name → website domain → email domain
    if name_key and name_key in accounts_map:
        return accounts_map[name_key]
    if web_domain and web_domain in domains_map:
        return domains_map[web_domain]
    if email_domain and email_domain in domains_map:
        return domains_map[email_domain]

    # If no match but company present, create and return
    if company:
        extra_account = {
            "websiteurl": website,
            "address1_city": sanitize(row.get("city")),
            "address1_stateorprovince": sanitize(row.get("state")),
            "address1_country": sanitize(row.get("country")),
        }
        return upsert_account(company, accounts_map, domains_map, extra=extra_account)

    # No resolvable/creatable account
    return None


# --- File discovery ---
def discover_wiza_csvs():
    downloads = os.path.join(os.path.expanduser("~"), "Downloads")
    return [os.path.join(downloads, f) for f in os.listdir(downloads)
            if f.startswith("WIZA") and f.lower().endswith(".csv")]


# --- Archive original file ---
def archive_original_file(src_path):
    digest_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Digest")
    os.makedirs(digest_dir, exist_ok=True)
    dest_path = os.path.join(digest_dir, os.path.basename(src_path))
    try:
        shutil.move(src_path, dest_path)
        print(f"📦 Original file moved to Digest/: {os.path.basename(src_path)}")
    except Exception as e:
        print(f"❌ Failed to move {src_path} to Digest/: {e}")


# --- Main ingestion for one file ---
def ingest_wiza_file(file_path, accounts_map, domains_map, contacts_by_email, contacts_by_fullname):
    print(f"\n📄 Processing: {os.path.basename(file_path)}")
    df = pd.read_csv(file_path)
    df = df.astype(object).where(pd.notnull(df), None)
    df = normalize_headers(df)

    created_contacts = 0
    skipped_contacts = 0
    failures = 0

    for _, row in df.iterrows():
        try:
            firstname = sanitize(row.get("firstname"))
            lastname = sanitize(row.get("lastname"))
            email = sanitize(row.get("emailaddress1"))
            jobtitle_raw = sanitize(row.get("jobtitle"))
            jobtitle = normalize_title(jobtitle_raw)
            list_name = sanitize(row.get("list_name"))
            leadtype = classify_leadtype(list_name)

            # Skip non-English names
            if is_non_english(firstname) or is_non_english(lastname):
                skipped_contacts += 1
                print(f"⏭️ Skipped non-English contact: {firstname} {lastname}")
                continue

            # Resolve or create account
            account_id = resolve_account_id(row, accounts_map, domains_map)
            if not account_id:
                skipped_contacts += 1
                print("⏭️ Skipped contact: no resolvable/creatable account")
                continue

            fullname = f"{firstname or ''} {lastname or ''}".strip() or None
            contact_payload = {
                "firstname": firstname,
                "lastname": lastname,
                "fullname": fullname,
                "jobtitle": jobtitle,
                "emailaddress1": email,
                "cr21a_leadtype": leadtype,
                "parentcustomerid_account@odata.bind": f"/accounts({account_id})"
            }
            contact_payload = {k: v for k, v in contact_payload.items() if v not in (None, "")}

            if not contact_payload.get("emailaddress1") and not contact_payload.get("fullname"):
                skipped_contacts += 1
                print("⏭️ Skipped contact (no email/fullname)")
                continue

            before_email_count = len(contacts_by_email)
            before_full_count = len(contacts_by_fullname)
            _cid = upsert_contact(contact_payload, contacts_by_email, contacts_by_fullname)
            after_email_count = len(contacts_by_email)
            after_full_count = len(contacts_by_fullname)

            if after_email_count > before_email_count or after_full_count > before_full_count:
                created_contacts += 1
            else:
                skipped_contacts += 1

        except Exception as e:
            failures += 1
            print(f"❌ Row failed: {e}")

    print(f"📊 Summary for {os.path.basename(file_path)}: "
          f"{created_contacts} contacts created, {skipped_contacts} contacts skipped, {failures} failures")


# --- Orchestration ---
def main():
    files = discover_wiza_csvs()
    if not files:
        print("ℹ️ No WIZA*.csv files found in Downloads. Exiting.")
        return

    accounts_map, domains_map = fetch_all_accounts()
    contacts_by_email, contacts_by_fullname = fetch_all_contacts()

    for fp in files:
        ingest_wiza_file(fp, accounts_map, domains_map, contacts_by_email, contacts_by_fullname)
        archive_original_file(fp)

    print("\n✅ Wiza ingestion run complete.")


if __name__ == "__main__":
    main()