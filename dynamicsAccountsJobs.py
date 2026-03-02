import os
import shutil
import time
import argparse
import math
import numpy as np
import pandas as pd
import urllib.parse
from datetime import datetime, timedelta, date

from dynamics_client import get_session, DYNAMICS_API, sanitize, extract_domain
from accountExport import log_account_for_export, export_accounts

# --- Utility: Convert Excel serial date to ISO string ---
def excel_serial_to_iso(value):
    try:
        if pd.isna(value):
            return None
        if isinstance(value, (datetime, pd.Timestamp)):
            return value.isoformat()
        if isinstance(value, str):
            try:
                return pd.to_datetime(value).isoformat()
            except Exception:
                return None
        val = float(value)
        if math.isnan(val) or math.isinf(val):
            return None
        base_date = datetime(1899, 12, 30)
        return (base_date + timedelta(days=val)).isoformat()
    except Exception:
        return None

# --- Preload all accounts into memory ---
def preload_accounts(session):
    print("📥 Preloading all accounts from Dynamics...")
    accounts_cache = {}
    url = f"{DYNAMICS_API}/accounts?$select=accountid,name,websiteurl"

    while url:
        res = session.get(url)
        if not res.ok:
            raise RuntimeError(f"Accounts fetch failed: {res.status_code} {res.text}")
        data = res.json()

        for a in data.get("value", []):
            name = a.get("name")
            if name:
                key = name.strip().lower()
                accounts_cache[key] = {
                    "accountid": a.get("accountid"),
                    "name": name,
                    "websiteurl": a.get("websiteurl"),
                }

        url = data.get("@odata.nextLink")

    print(f"✅ Loaded {len(accounts_cache)} accounts into cache")
    return accounts_cache

# --- Account Upsert (cache-first, API only for creates) ---
def upsert_account(session, account_obj, accounts_cache):
    company_name = account_obj.get("name")
    cache_key = company_name.strip().lower() if company_name else ""

    # Check in-memory cache first
    cached = accounts_cache.get(cache_key)
    if cached:
        account_id = cached["accountid"]

        # Domain cross-check: compare CSV website to cached Dynamics record
        csv_domain = extract_domain(account_obj.get("websiteurl"))
        dynamics_domain = extract_domain(cached.get("websiteurl"))
        if csv_domain and dynamics_domain and csv_domain != dynamics_domain:
            print(f"⚠️ Domain mismatch for '{company_name}': CSV={csv_domain}, Dynamics={dynamics_domain}")

        account_obj["Account Id"] = account_id
        log_account_for_export(account_obj)
        return account_obj

    # Not in cache — create via API
    print(f"➕ Creating new Account: {company_name}")
    payload = {
        k: v for k, v in account_obj.items()
        if v not in (None, "") and k != "Account Id"
    }

    create_res = session.post(f"{DYNAMICS_API}/accounts", json=payload)
    if not create_res.ok:
        raise RuntimeError(f"Account creation failed: {create_res.status_code} {create_res.text}")

    entity_id = create_res.headers.get("OData-EntityId")
    account_id = entity_id.split("(")[1].split(")")[0]
    print(f"✅ Created Account: {company_name} (ID={account_id})")

    # Add to cache for subsequent rows
    accounts_cache[cache_key] = {
        "accountid": account_id,
        "name": company_name,
        "websiteurl": account_obj.get("websiteurl"),
    }

    account_obj["Account Id"] = account_id
    log_account_for_export(account_obj)
    return account_obj

# --- Contact Upsert (scoped to account) ---
def upsert_contact(session, contact_name, account_id):
    if not contact_name or str(contact_name).strip() == "":
        return None

    print(f"🔍 Looking up Contact: {contact_name} (scoped to account {account_id})")
    contact_safe = str(contact_name).replace("'", "''")
    filter_str = f"fullname eq '{contact_safe}' and _parentcustomerid_value eq {account_id}"
    query = urllib.parse.quote(filter_str, safe="= '")
    url = f"{DYNAMICS_API}/contacts?$filter={query}"

    res = session.get(url)
    if res.ok and res.json().get("value"):
        contact_id = res.json()["value"][0]["contactid"]
        print(f"✅ Found existing Contact at this account: {contact_name} (ID={contact_id})")
        return contact_id

    print(f"➕ Creating new Contact: {contact_name}")
    parts = str(contact_name).split(" ")
    contact = {
        "firstname": parts[0],
        "lastname": " ".join(parts[1:]) if len(parts) > 1 else "",
        "fullname": str(contact_name),
        "parentcustomerid_account@odata.bind": f"/accounts({account_id})"
    }
    create_res = session.post(f"{DYNAMICS_API}/contacts", json=contact)
    if not create_res.ok:
        raise RuntimeError(f"Contact creation failed: {create_res.status_code} {create_res.text}")

    entity_id = create_res.headers.get("OData-EntityId")
    contact_id = entity_id.split("(")[1].split(")")[0]
    print(f"✅ Created Contact: {contact_name} (ID={contact_id})")
    return contact_id

# --- Preload existing job links ---
def preload_existing_joblinks(session):
    existing_links = set()
    url = f"{DYNAMICS_API}/cr21a_jobpostings?$select=cr21a_joblink"

    while url:
        res = session.get(url)
        if not res.ok:
            raise RuntimeError(f"Failed to fetch job links: {res.status_code} {res.text}")

        for job in res.json().get("value", []):
            link = job.get("cr21a_joblink")
            if link:
                existing_links.add(link.strip())

        url = res.json().get("@odata.nextLink")

    print(f"✅ Loaded {len(existing_links)} job links")
    return existing_links

# --- Job Create ---
def create_job(session, row, account_id, contact_id=None, existing_links=None):
    job_title = row.get("Job Title")
    company_name = row.get("Company Name")
    job_link_raw = row.get("Job Link", "")
    job_link = str(job_link_raw).strip() if job_link_raw is not None else ""

    # --- Uniqueness check by job link (ignore empty or "nan") ---
    if job_link and job_link.lower() != "nan" and existing_links is not None:
        if job_link in existing_links:
            print(f"Skipped duplicate job: {job_title} at {company_name}")
            return
        existing_links.add(job_link)

    field_map = {
        "cr21a_jobtitle": "Job Title",
        "cr21a_companyname": "Company Name",
        "cr21a_location": "Location",
        "cr21a_joblink": "Job Link",
        "cr21a_source": "Source",
        "cr21a_tags": "Tags",
    }
    job = {dynamics_field: sanitize(row.get(csv_column))
           for dynamics_field, csv_column in field_map.items()}

    date_fields = {
        "cr21a_dateadded": "Date Added (UTC)",
        "cr21a_dateapplied": "Date Applied (UTC)",
        "cr21a_dateinterviewed": "Date Interviewed (UTC)",
        "cr21a_dateoffered": "Date Offered (UTC)",
        "cr21a_daterejected": "Date Rejected (UTC)",
    }
    for dynamics_field, csv_column in date_fields.items():
        job[dynamics_field] = excel_serial_to_iso(row.get(csv_column))

    # --- Normalize payload: replace NaN/Inf and "nan" strings with None ---
    for k, v in list(job.items()):
        if v is None:
            continue
        if pd.isna(v):
            job[k] = None
            continue
        if isinstance(v, (float, np.floating)):
            if not math.isfinite(float(v)):
                job[k] = None
                continue
        if isinstance(v, str) and v.strip().lower() == "nan":
            job[k] = None
            continue

    # Required bindings
    job["cr21a_jobposting@odata.bind"] = f"/accounts({account_id})"
    if contact_id:
        job["cr21a_jobposting_Contact@odata.bind"] = f"/contacts({contact_id})"

    res = session.post(f"{DYNAMICS_API}/cr21a_jobpostings", json=job)
    if not res.ok:
        raise RuntimeError(f"Job creation failed: {res.status_code} {res.text}")

    print(f"✅ Created Job: {job_title} at {company_name}")

# --- Ingest a file ---
def _parse_row_date(value):
    """Extract just the date from a Date Added (UTC) cell value."""
    iso = excel_serial_to_iso(value)
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso).date()
    except Exception:
        return None

def ingest_file(session, file_path, existing_links, accounts_cache, date_filter=None):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        df = pd.read_csv(file_path)
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path)
    else:
        print(f"⚠️ Unsupported file type: {ext}")
        return False

    # --- Normalize DataFrame to avoid NaN/NaT leaking into JSON ---
    df = df.astype(object).where(pd.notnull(df), None)

    if date_filter:
        print(f"📅 Date filter active: only processing rows with Date Added = {date_filter}")

    success_count, fail_count, skipped_count, skipped_date = 0, 0, 0, 0
    for _, row in df.iterrows():
        try:
            # --- Date filter ---
            if date_filter:
                row_date = _parse_row_date(row.get("Date Added (UTC)"))
                if row_date != date_filter:
                    skipped_date += 1
                    continue
            # Build Dynamics-friendly account object from row
            account_obj = {
                "name": row.get("Company Name"),
                "websiteurl": row.get("Website URL"),
                "address1_country": row.get("Country"),
                "address1_city": (str(row.get("City") or row.get("Location") or "")[:80] or None),
                "address1_line1": row.get("Street"),
                "address1_stateorprovince": row.get("State"),
                "address1_postalcode": row.get("Zip/Postal Code"),
                "industrycode": row.get("Industry"),
                "tickersymbol": row.get("Stock Symbol"),
            }

            # Upsert account (cache-first, API only for creates)
            account_obj = upsert_account(session, account_obj, accounts_cache)

            # Use enriched account_obj downstream
            account_id = account_obj["Account Id"]
            contact_id = upsert_contact(session, row.get("Contact Name"), account_id)

            # create_job checks uniqueness by job link
            before_count = len(existing_links)
            create_job(session, row, account_id, contact_id, existing_links)
            after_count = len(existing_links)

            if after_count == before_count:
                skipped_count += 1
            else:
                success_count += 1

        except Exception as e:
            fail_count += 1
            print(f"❌ Error processing {row.get('Job Title')} at {row.get('Company Name')}: {e}")

    summary = f"📊 File summary: {success_count} jobs created, {skipped_count} duplicates skipped, {fail_count} failures"
    if date_filter:
        summary += f", {skipped_date} skipped by date filter"
    print(summary)
    return success_count > 0

# --- Robust move with retry ---
def move_with_retry(src, dst, retries=3, delay=1.0):
    for attempt in range(1, retries + 1):
        try:
            shutil.move(src, dst)
            return True
        except Exception as e:
            print(f"⚠️ Move failed (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)

    try:
        shutil.copy2(src, dst)
        os.remove(src)
        return True
    except Exception as e:
        print(f"❌ Fallback copy/remove failed: {e}")
        return False

# --- Process all files ---
def process_all_files(date_filter=None):
    session = get_session()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    ingest_dir = os.path.join(base_dir, "Data", "Ingest")
    digest_dir = os.path.join(base_dir, "Data", "Digest")

    os.makedirs(ingest_dir, exist_ok=True)
    os.makedirs(digest_dir, exist_ok=True)

    all_files = os.listdir(ingest_dir)
    files = [f for f in all_files if f.lower().endswith((".csv", ".xlsx", ".xls"))]

    if not files:
        print("ℹ️ No CSV/XLSX files found in Ingest. Exiting.")
        return

    # --- preload once per run ---
    existing_links = preload_existing_joblinks(session)
    accounts_cache = preload_accounts(session)

    for filename in files:
        src_path = os.path.join(ingest_dir, filename)
        processed = ingest_file(session, src_path, existing_links, accounts_cache, date_filter)

        dest_path = os.path.join(digest_dir, filename)
        moved = move_with_retry(src_path, dest_path)
        if moved:
            status = "processed" if processed else "processed-with-errors"
        else:
            print(f"❌ Failed to archive file: {filename}. Please check locks/permissions.")

    # Export accounts at the end of the run
    print("📤 Exporting Accounts touched in this run...")
    export_accounts()

# Run ingestion
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest job postings from CSV/XLSX into Dynamics")
    parser.add_argument("--date", type=str, default=None,
                        help="Only ingest rows with this Date Added (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--all", action="store_true", dest="all_dates",
                        help="Disable date filtering, ingest all rows.")
    args = parser.parse_args()

    if args.all_dates:
        date_filter = None
    elif args.date:
        date_filter = date.fromisoformat(args.date)
    else:
        date_filter = date.today()

    process_all_files(date_filter)
