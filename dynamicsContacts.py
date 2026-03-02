import os
import shutil
import pandas as pd

from dynamics_client import get_session, DYNAMICS_API, sanitize, extract_domain

# --- Wiza-specific utilities ---
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

def is_non_english(text):
    if not text:
        return False
    try:
        text.encode("ascii")
        return False
    except UnicodeEncodeError:
        return True


# --- Preload Dynamics data ---
def fetch_all_accounts(session):
    print("📥 Fetching all Accounts...")
    accounts, domains = {}, {}
    url = f"{DYNAMICS_API}/accounts?$select=accountid,name,websiteurl"

    while url:
        res = session.get(url)
        if not res.ok:
            raise RuntimeError(f"Accounts fetch failed: {res.status_code} {res.text}")
        data = res.json()

        for a in data.get("value", []):
            accid = a.get("accountid")
            name = sanitize(a.get("name"))
            web = sanitize(a.get("websiteurl"))

            if name and accid:
                accounts[_norm_name(name)] = accid

            dom = extract_domain(web)
            if dom:
                domains[dom] = accid

        url = data.get("@odata.nextLink")

    print(f"✅ Loaded {len(accounts)} accounts; {len(domains)} domains")
    return accounts, domains


def fetch_all_contacts(session):
    print("📥 Fetching all Contacts...")
    contacts_by_email = {}
    contacts_by_fullname = {}

    url = f"{DYNAMICS_API}/contacts?$select=contactid,fullname,emailaddress1"
    while url:
        res = session.get(url)
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
def upsert_account(session, name, accounts_map, domains_map, extra=None):
    key = _norm_name(name)
    if key in accounts_map:
        return accounts_map[key]

    payload = {"name": sanitize(name)}
    if extra:
        for k, v in extra.items():
            sv = sanitize(v)
            if sv:
                payload[k] = sv

    res = session.post(f"{DYNAMICS_API}/accounts", json=payload)
    if not res.ok:
        raise RuntimeError(f"Account creation failed: {res.status_code} {res.text}")

    entity_id = res.headers.get("OData-EntityId")
    account_id = entity_id.split("(")[1].split(")")[0]

    accounts_map[key] = account_id

    dom = extract_domain(extra.get("websiteurl")) if extra else None
    if dom:
        domains_map[dom] = account_id

    print(f"➕ Account created: {name} (ID={account_id})")
    return account_id


def upsert_contact(session, payload, email_map, fullname_map):
    email = sanitize(payload.get("emailaddress1"))
    fullname = sanitize(payload.get("fullname"))

    cid = None
    if email:
        cid = email_map.get(email.lower())
    if not cid and fullname:
        cid = fullname_map.get(fullname.lower())

    if cid:
        # Update existing contact (OK even if email missing now; we're just enforcing on create)
        res = session.patch(f"{DYNAMICS_API}/contacts({cid})", json=payload)
        if not res.ok:
            raise RuntimeError(f"Contact update failed: {res.status_code} {res.text}")
        return cid

    # ❗ Safety net: do NOT create a new contact without an email
    if not email:
        raise RuntimeError("Attempted to create a new contact without emailaddress1")

    # Create new
    res = session.post(f"{DYNAMICS_API}/contacts", json=payload)
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
def resolve_account_id(session, row, accounts_map, domains_map):
    company = sanitize(row.get("accountname"))
    website = sanitize(row.get("websiteurl"))
    email = sanitize(row.get("emailaddress1"))

    name_key = _norm_name(company)
    web_domain = extract_domain(website)
    email_domain = extract_domain(email)

    # Prefer website domain; fall back to email domain
    domain_key = web_domain or email_domain

    existing_by_name = accounts_map.get(name_key) if name_key else None
    existing_by_domain = domains_map.get(domain_key) if domain_key else None

    # ✅ Both name and domain resolve to the same account
    if existing_by_name and existing_by_domain and existing_by_name == existing_by_domain:
        print(
            f"🔗 Perfect match on name+domain -> "
            f"company='{company}', domain='{domain_key}', account_id={existing_by_name}"
        )
        return existing_by_name

    # ✅ Name matches an account that has NO domain yet — trust the name,
    #    and backfill the website on the Dynamics record so future runs get
    #    a perfect name+domain match.
    if existing_by_name and not existing_by_domain and domain_key:
        # Make sure no OTHER account already owns this domain
        if domain_key not in domains_map:
            print(
                f"🔗 Name match (no domain on record) -> "
                f"company='{company}', domain='{domain_key}', account_id={existing_by_name}. "
                f"Backfilling websiteurl."
            )
            # Backfill websiteurl on Dynamics account
            patch_url = f"{DYNAMICS_API}/accounts({existing_by_name})"
            patch_resp = session.patch(patch_url, json={"websiteurl": website or f"https://{domain_key}"})
            if patch_resp.ok:
                domains_map[domain_key] = existing_by_name
                print(f"   ✅ Backfilled websiteurl with domain '{domain_key}'")
            else:
                print(f"   ⚠️ Backfill failed: {patch_resp.status_code} {patch_resp.text}")
            return existing_by_name

    # ⚠️ Domain matches but name doesn't — possible subsidiary or mismatch
    if existing_by_domain and not existing_by_name:
        print(
            f"⚠️ Domain '{domain_key}' matches account_id={existing_by_domain} but "
            f"company name '{company}' does not match. Skipping to be safe."
        )
        return None

    # ⚠️ Name and domain both match but to DIFFERENT accounts — conflict
    if existing_by_name and existing_by_domain and existing_by_name != existing_by_domain:
        print(
            f"⚠️ Conflict: name '{company}' -> {existing_by_name}, "
            f"domain '{domain_key}' -> {existing_by_domain}. Skipping."
        )
        return None

    # ❌ No match at all
    print(
        f"⏭️ No match for company='{company}', "
        f"domain='{domain_key}'. Not linking or creating."
    )
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
def ingest_wiza_file(session, file_path, accounts_map, domains_map, email_map, fullname_map):
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

            # ❗ HARD REQUIREMENT: must have an email to proceed
            if not email:
                skipped += 1
                print(
                    f"Skipping row (no email): "
                    f"{(firstname or '')} {(lastname or '')} | "
                    f"company={row.get('accountname')} | raw_email={row.get('emailaddress1')}"
                )
                continue

            if is_non_english(firstname) or is_non_english(lastname):
                skipped += 1
                continue

            account_id = resolve_account_id(session, row, accounts_map, domains_map)
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

            # Drop falsy values
            payload = {k: v for k, v in payload.items() if v}

            cid = upsert_contact(session, payload, email_map, fullname_map)

            # Attach account
            ref = f"{DYNAMICS_API}/contacts({cid})/parentcustomerid_account/$ref"
            ref_payload = {"@odata.id": f"{DYNAMICS_API}/accounts({account_id})"}

            ref_res = session.put(ref, json=ref_payload)
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

    session = get_session()

    accounts_map, domains_map = fetch_all_accounts(session)
    email_map, fullname_map = fetch_all_contacts(session)

    for fp in files:
        ingest_wiza_file(session, fp, accounts_map, domains_map, email_map, fullname_map)
        archive_original_file(fp)

    print("\n✅ Wiza ingestion complete.")


if __name__ == "__main__":
    main()
