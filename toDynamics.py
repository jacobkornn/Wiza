import os
import shutil
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from msal import ConfidentialClientApplication
from dotenv import load_dotenv
import urllib.parse

# --- Load environment variables ---
load_dotenv()

# --- Acquire Dynamics Token ---
def get_dynamics_token():
    print("🔑 Acquiring Dynamics access token...")
    app = ConfidentialClientApplication(
        client_id=os.getenv("DYNAMICS_CLIENT_ID"),
        client_credential=os.getenv("DYNAMICS_CLIENT_SECRET"),
        authority=f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}"
    )
    token = app.acquire_token_for_client(
        scopes=[f"{os.getenv('DYNAMICS_ORG_URL')}/.default"]
    )
    if "access_token" not in token:
        raise RuntimeError(f"Token request failed: {token}")
    print("✅ Token acquired successfully")
    return token["access_token"]

ACCESS_TOKEN = get_dynamics_token()

DYNAMICS_BASE_URL = f"{os.getenv('DYNAMICS_ORG_URL')}/api/data/v9.2"
AUTH_HEADER = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# --- Utility: Convert Excel serial date to ISO string ---
def excel_serial_to_iso(serial):
    try:
        if pd.isna(serial):
            return None
        base_date = datetime(1899, 12, 30)
        return (base_date + timedelta(days=float(serial))).isoformat()
    except Exception:
        return None

# --- Account Upsert ---
def upsert_account(company_name, location):
    print(f"🔍 Looking up Account: {company_name}")
    company_safe = company_name.replace("'", "''")
    filter_str = f"name eq '{company_safe}'"
    query = urllib.parse.quote(filter_str, safe="= '")
    url = f"{DYNAMICS_BASE_URL}/accounts?$filter={query}"

    res = requests.get(url, headers=AUTH_HEADER)
    if res.ok and res.json().get("value"):
        account_id = res.json()["value"][0]["accountid"]
        print(f"✅ Found existing Account: {company_name} (ID={account_id})")
        return account_id

    print(f"➕ Creating new Account: {company_name}")
    account = {"name": company_name}
    if location:
        account["address1_city"] = location

    create_res = requests.post(f"{DYNAMICS_BASE_URL}/accounts", json=account, headers=AUTH_HEADER)
    if not create_res.ok:
        raise RuntimeError(f"Account creation failed: {create_res.status_code} {create_res.text}")

    entity_id = create_res.headers.get("OData-EntityId")
    account_id = entity_id.split("(")[1].split(")")[0]
    print(f"✅ Created Account: {company_name} (ID={account_id})")
    return account_id

# --- Contact Upsert ---
def upsert_contact(contact_name, account_id):
    if not contact_name or str(contact_name).strip() == "":
        print("ℹ️ No Contact provided, skipping...")
        return None

    print(f"🔍 Looking up Contact: {contact_name}")
    contact_safe = str(contact_name).replace("'", "''")
    filter_str = f"fullname eq '{contact_safe}'"
    query = urllib.parse.quote(filter_str, safe="= '")
    url = f"{DYNAMICS_BASE_URL}/contacts?$filter={query}"

    res = requests.get(url, headers=AUTH_HEADER)
    if res.ok and res.json().get("value"):
        contact_id = res.json()["value"][0]["contactid"]
        print(f"✅ Found existing Contact: {contact_name} (ID={contact_id})")
        return contact_id

    print(f"➕ Creating new Contact: {contact_name}")
    parts = str(contact_name).split(" ")
    contact = {
        "firstname": parts[0],
        "lastname": " ".join(parts[1:]) if len(parts) > 1 else "",
        "fullname": str(contact_name),
        "parentcustomerid_account@odata.bind": f"/accounts({account_id})"
    }
    create_res = requests.post(f"{DYNAMICS_BASE_URL}/contacts", json=contact, headers=AUTH_HEADER)
    if not create_res.ok:
        raise RuntimeError(f"Contact creation failed: {create_res.status_code} {create_res.text}")

    entity_id = create_res.headers.get("OData-EntityId")
    contact_id = entity_id.split("(")[1].split(")")[0]
    print(f"✅ Created Contact: {contact_name} (ID={contact_id})")
    return contact_id

# --- Job Create ---
def create_job(row, account_id, contact_id=None):
    print(f"➕ Creating Job: {row.get('Job Title')} at {row.get('Company Name')}")
    job = {
        "cr21a_jobposting_jobtitle": row.get("Job Title"),
        "cr21a_jobposting_status": row.get("Status"),
        "cr21a_jobposting_salary": row.get("Salary"),
        "cr21a_jobposting_location": row.get("Location"),
        "cr21a_jobposting_joblink": row.get("Job Link"),
        "cr21a_jobposting_source": row.get("Source"),
        "cr21a_jobposting_tags": row.get("Tags"),
        "cr21a_jobposting_dateadded": excel_serial_to_iso(row.get("Date Added (UTC)")),
        "cr21a_jobposting_dateapplied": excel_serial_to_iso(row.get("Date Applied (UTC)")),
        "cr21a_jobposting_dateinterviewed": excel_serial_to_iso(row.get("Date Interviewed (UTC)")),
        "cr21a_jobposting_dateoffered": excel_serial_to_iso(row.get("Date Offered (UTC)")),
        "cr21a_jobposting_daterejected": excel_serial_to_iso(row.get("Date Rejected (UTC)")),
        "cr21a_jobposting_Account@odata.bind": f"/accounts({account_id})"
    }
    if contact_id:
        job["cr21a_jobposting_Contact@odata.bind"] = f"/contacts({contact_id})"

    create_res = requests.post(f"{DYNAMICS_BASE_URL}/cr21a_jobpostings", json=job, headers=AUTH_HEADER)
    if not create_res.ok:
        raise RuntimeError(f"Job creation failed: {create_res.status_code} {create_res.text}")

    print(f"✅ Created Job: {row.get('Job Title')} at {row.get('Company Name')}")

# --- Ingest a file (CSV or Excel) ---
def ingest_file(file_path):
    print(f"📂 Reading file: {file_path}")
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        df = pd.read_csv(file_path)
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path)
    else:
        print(f"⚠️ Unsupported file type: {ext}")
        return False

    success_count, fail_count = 0, 0
    for _, row in df.iterrows():
        try:
            account_id = upsert_account(row["Company Name"], row.get("Location"))
            contact_id = upsert_contact(row.get("Contact Name"), account_id)
            create_job(row, account_id, contact_id)
            success_count += 1
        except Exception as e:
            fail_count += 1
            print(f"❌ Error processing {row.get('Job Title')} at {row.get('Company Name')}: {e}")

    print(f"📊 File summary: {success_count} jobs created, {fail_count} failures")
    return success_count > 0

# --- Robust move with retry ---
def move_with_retry(src, dst, retries=3, delay=1.0):
    print(f"➡️ Move attempt: {src} → {dst}")
    for attempt in range(1, retries + 1):
        try:
            shutil.move(src, dst)
            print(f"📦 Moved successfully on attempt {attempt}: {dst}")
            return True
        except Exception as e:
            print(f"⚠️ Move failed (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)

    # Fallback: copy then remove
    try:
        shutil.copy2(src, dst)
        os.remove(src)
        print(f"📦 Copied and removed source as fallback: {dst}")
        return True
    except Exception as e:
        print(f"❌ Fallback copy/remove failed: {e}")
        return False

def process_all_files():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    ingest_dir = os.path.join(base_dir, "Data", "Ingest")
    digest_dir = os.path.join(base_dir, "Data", "Digest")

    os.makedirs(ingest_dir, exist_ok=True)
    os.makedirs(digest_dir, exist_ok=True)

    print(f"🔎 Ingest directory: {ingest_dir}")
    print(f"🔎 Digest directory: {digest_dir}")

    # Show all files present
    all_files = os.listdir(ingest_dir)
    print(f"📂 Files currently in ingest dir: {all_files}")

    # Only keep supported file types
    files = [f for f in all_files if f.lower().endswith((".csv", ".xlsx", ".xls"))]
    print(f"📄 Found {len(files)} supported file(s): {files}")

    if not files:
        print("ℹ️ No CSV/XLSX files found in Ingest. Exiting.")
        return

    for filename in files:
        src_path = os.path.join(ingest_dir, filename)
        print(f"🚀 Starting ingestion for {filename}")
        processed = ingest_file(src_path)

        dest_path = os.path.join(digest_dir, filename)
        print(f"🧭 Move source: {src_path}")
        print(f"🧭 Move destination: {dest_path}")

        moved = move_with_retry(src_path, dest_path)
        if moved:
            status = "processed" if processed else "processed-with-errors"
            print(f"✅ Archived file ({status}): {dest_path}")
        else:
            print(f"❌ Failed to archive file: {filename}. Please check locks/permissions.")

# Run ingestion
if __name__ == "__main__":
    process_all_files()