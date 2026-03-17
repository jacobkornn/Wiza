import os
import re
import argparse
import traceback
import pandas as pd
from datetime import datetime, timezone
from win32com.client import Dispatch
from dotenv import load_dotenv

from dynamics_client import get_session, DYNAMICS_API

load_dotenv()

# ----------------- CONFIG ----------------- #
OUTLOOK_ACCOUNT = os.getenv("OUTLOOK_ACCOUNT", "jake.korn@theboxk.com")
CUSTOM_FOLDER_NAME = os.getenv("CUSTOM_FOLDER_NAME", "JakeJobs Outbound")

# ----------------- Templates ----------------- #
SALES_TEMPLATE = """<html><body>
<p>Hi {firstname},</p>
<p>Hope this message finds you well! I recently applied for the {cr21a_jobtitle} position at {account_name} and wanted to reach out directly.
With a CS background and startup experience applying machine learning to sales and marketing,
I bring technical depth plus strong communication skills.
Given your role as {contact_jobtitle}, I'd love to connect and learn more about navigating potential opportunities at {account_name}.</p>
<p>Best,<br>{your_full_name}</p>
<p><br>(425) 354-0440<br>
<a href="https://www.linkedin.com/in/jacob-korn-3aa792248/">My LinkedIn</a></p>
</body></html>"""

ENGINEERING_TEMPLATE = """<html><body>
<p>Hi {firstname},</p>
<p>Hope this message finds you well! I recently applied for the {cr21a_jobtitle} position at {account_name} and wanted to reach out directly.
I am a software engineer with entrepreneurial experience building and applying machine learning to CRM data workflows.
Given your role as {contact_jobtitle}, I'd love to connect and learn more about navigating potential opportunities at {account_name}.</p>
<p>Best,<br>{your_full_name}</p>
<p><br>(425) 354-0440<br>
<a href="https://www.linkedin.com/in/jacob-korn-3aa792248/">My LinkedIn</a></p>
</body></html>"""

# ----------------- Helper Functions ----------------- #
def normalize_email(addr):
    if pd.isna(addr):
        return ""

    e = str(addr).strip()
    e = re.sub(r'^mailto:', '', e, flags=re.I).strip()

    # Safely remove BOTH types of quotes ONLY at the edges
    e = e.strip('"').strip("'")

    return e.lower()


def strip_html_tags(text):
    return re.sub(r'<[^>]+>', '', text)

# ----------------- Dynamics data loaders (rely on session) ----------------- #
def load_accounts_with_jobs(session):
    print("Loading accounts with expanded job postings...")
    url = (
        f"{DYNAMICS_API}/accounts"
        f"?$select=accountid,name"
        f"&$expand=cr21a_Account_to_JobPosting($select=cr21a_jobpostingid,cr21a_jobtitle)"
    )
    resp = session.get(url)
    resp.raise_for_status()
    accounts = resp.json().get("value", [])

    account_map = {a["accountid"]: a for a in accounts}
    job_to_account = {}
    for a in accounts:
        jobs = a.get("cr21a_Account_to_JobPosting") or []
        for j in jobs:
            job_to_account[j["cr21a_jobpostingid"]] = a

    print(f"Loaded {len(account_map)} accounts; mapped {len(job_to_account)} jobs to accounts")
    return account_map, job_to_account

def load_all_contacts_by_account(session):
    print("Loading all contacts... (with createdon)")
    # Select important fields including createdon
    url = (
        f"{DYNAMICS_API}/contacts"
        f"?$select=contactid,firstname,lastname,fullname,emailaddress1,jobtitle,"
        f"cr21a_leadtype,_parentcustomerid_value,createdon"
    )
    resp = session.get(url)
    resp.raise_for_status()
    contacts = resp.json().get("value", [])

    contact_map = {}
    for c in contacts:
        acc_id = c.get("_parentcustomerid_value")
        if acc_id:
            contact_map.setdefault(acc_id, []).append(c)

    total_contacts = sum(len(v) for v in contact_map.values())
    print(f"Indexed {total_contacts} contacts across {len(contact_map)} accounts")
    return contact_map

def load_all_jobs(session):
    print("Loading all job postings...")
    url = f"{DYNAMICS_API}/cr21a_jobpostings?$select=cr21a_jobpostingid,cr21a_jobtitle"
    resp = session.get(url)
    resp.raise_for_status()
    jobs = resp.json().get("value", [])
    print(f"Loaded {len(jobs)} job postings")
    return jobs

# ----------------- System user lookup ----------------- #
def find_systemuser_id_by_internal_email(session, email):
    """
    Lookup systemuser by internalemailaddress. Returns systemuserid GUID string.
    Raises Exception if not found.
    """
    if not email:
        raise ValueError("Email must be provided to lookup systemuser")

    url = f"{DYNAMICS_API}/systemusers"
    # OData filter with single quotes around the email (escape any single quotes inside the email)
    safe_email = email.replace("'", "''")
    params = {
        "$select": "systemuserid,internalemailaddress",
        "$filter": f"internalemailaddress eq '{safe_email}'"
    }
    resp = session.get(url, params=params)
    resp.raise_for_status()
    items = resp.json().get("value", [])
    if not items:
        raise Exception(f"No systemuser found with internalemailaddress = {email}")
    # return first match
    systemuser_id = items[0]["systemuserid"]
    print(f"Found systemuser id {systemuser_id} for email {email}")
    return systemuser_id

def preload_contacted_contacts(session):
    """Load all contact IDs that appear as a TO recipient on any outgoing email activity."""
    print("📥 Preloading already-contacted contacts from Dynamics email activity parties...")
    contacted = set()
    # participationtypemask 2 = TO recipient; filter to contact party records only
    url = (
        f"{DYNAMICS_API}/activityparties"
        f"?$select=_partyid_value,_activityid_value"
        f"&$filter=participationtypemask eq 2"
        f" and _partyid_value ne null"
    )
    while url:
        resp = session.get(url)
        if not resp.ok:
            raise RuntimeError(f"Failed to fetch activity parties: {resp.status_code} {resp.text}")
        data = resp.json()
        for party in data.get("value", []):
            cid = party.get("_partyid_value")
            if cid:
                contacted.add(cid)
        url = data.get("@odata.nextLink")
    print(f"✅ Found {len(contacted)} already-contacted contacts")
    return contacted

def log_email_to_dynamics(session, contact_id, jobposting_id, subject, body, sender_systemuser_id):
    """
    Log the email as an activity in Dynamics using the provided session.
    Creates required email_activity_parties so the email appears in Activities.
    """
    try:
        payload = {
            "subject": subject,
            "description": body,
            "directioncode": True,  # outgoing

            # Activity parties: FROM (systemuser) and TO (contact)
            "email_activity_parties": [
                {
                    "partyid_systemuser@odata.bind": f"/systemusers({sender_systemuser_id})",
                    "participationtypemask": 1  # FROM
                },
                {
                    "partyid_contact@odata.bind": f"/contacts({contact_id})",
                    "participationtypemask": 2  # TO
                }
            ],

            # Regarding fields
            "regardingobjectid_contact@odata.bind": f"/contacts({contact_id})",
            "regardingobjectid_cr21a_jobposting@odata.bind": f"/cr21a_jobpostings({jobposting_id})"
        }
        url = f"{DYNAMICS_API}/emails"
        # use session to preserve Authorization header
        resp = session.post(url, json=payload, headers={"Content-Type": "application/json;odata.metadata=minimal"})
        if not resp.ok:
            print(f"Error logging email {resp.status_code}: {resp.text}")
        else:
            print("Logged email successfully (Dynamics email activity created)")
    except Exception:
        print("Exception when logging email to Dynamics:")
        traceback.print_exc()

# ----------------- Outlook Integration ----------------- #
def get_or_create_custom_folder(outlook, folder_name):
    namespace = outlook.GetNamespace("MAPI")
    root = namespace.Folders[OUTLOOK_ACCOUNT]
    try:
        target_folder = root.Folders.Item(folder_name)
    except Exception:
        target_folder = root.Folders.Add(folder_name)
    return target_folder

# ----------------- Attachments / Templates / Email Builders ----------------- #
_ATTACHMENT_CACHE = {}

def select_documents_for_leadtype(leadtype):
    lt = str(leadtype).strip().lower()
    if lt == "sales":
        base_dir = os.path.join("Data", "Sales")
    elif lt == "engineering":
        base_dir = os.path.join("Data", "Software")
    else:
        base_dir = os.path.join("Data", "Software")

    resume = os.path.join(base_dir, "Jacob_Korn_Resume.pdf")
    cover = os.path.join(base_dir, "Jacob_Korn_CoverLetter.pdf")
    return [resume, cover]

def get_attachments_cached(leadtype):
    key = str(leadtype).strip().lower()
    if key not in _ATTACHMENT_CACHE:
        self_att = select_documents_for_leadtype(key)
        # resolve absolute paths once
        self_att = [os.path.abspath(p) for p in self_att]
        _ATTACHMENT_CACHE[key] = self_att
    return _ATTACHMENT_CACHE[key]

def build_email_body(contact, job, account, leadtype):
    template_data = {
        "firstname": contact.get("firstname", ""),
        "account_name": account.get("name", ""),
        "cr21a_jobtitle": job.get("cr21a_jobtitle", ""),
        "contact_jobtitle": contact.get("jobtitle", ""),
        "your_full_name": "Jacob Korn"
    }
    lt = str(leadtype or "").strip().lower()
    if lt == "sales":
        body = SALES_TEMPLATE.format(**template_data)
    elif lt == "engineering":
        body = ENGINEERING_TEMPLATE.format(**template_data)
    else:
        body = SALES_TEMPLATE.format(**template_data)
    subject = f"Introduction - Interested in {template_data['account_name']}"
    return subject, body

def preview_email(contact, job, account, subject, body, attachments):
    print("\n--- Contact ---")
    print(contact)
    print("\n--- Job Posting ---")
    print(job)
    print("\n--- Account ---")
    print(account)
    print("\n--- Subject ---")
    print(subject)
    print("\n--- Body ---")
    print(strip_html_tags(body))
    print("\n--- Attachments ---")
    for attachment in attachments:
        if os.path.exists(attachment):
            print(f"{attachment} (will be attached)")
        else:
            print(f"{attachment} (MISSING)")
    print("-" * 40)

def stage_email(outlook, contact, job, account, subject, body, attachments, target_folder, dynamics_session, sender_systemuser_id):
    try:
        print(f"Staging email to {contact.get('emailaddress1')}...")
        mail = outlook.CreateItem(0)
        mail.To = normalize_email(contact.get("emailaddress1"))
        mail.Subject = subject
        mail.HTMLBody = body
        for attachment in attachments:
            if os.path.exists(attachment):
                mail.Attachments.Add(attachment)
        mail.Save()
        mail.Move(target_folder)

        # Log to Dynamics using provided session and sender systemuser id
        log_email_to_dynamics(
            session=dynamics_session,
            contact_id=contact["contactid"],
            jobposting_id=job["cr21a_jobpostingid"],
            subject=subject,
            body=body,
            sender_systemuser_id=sender_systemuser_id
        )
        print("Staged email successfully")
    except Exception:
        print("Error staging email:")
        traceback.print_exc()
        raise

# ----------------- Main Workflow ----------------- #
def main(preview=False, new_only=False):
    print("Starting Dynamics email staging app...")
    if new_only:
        print("🆕 NEW-ONLY mode: skipping contacts that already have an outgoing email activity")

    # Cutoff date for createdon filter
    cutoff_date = datetime.now(timezone.utc).date()

    # Build single Dynamics session for all API traffic (token managed by dynamics_client)
    try:
        dynamics_session = get_session()
        dynamics_session.headers["Accept"] = "application/json;odata.metadata=minimal"
        dynamics_session.headers["Prefer"] = 'odata.include-annotations="*"'

        # Lookup the systemuser id by internalemailaddress (matching OUTLOOK_ACCOUNT)
        try:
            sender_systemuser_id = find_systemuser_id_by_internal_email(dynamics_session, OUTLOOK_ACCOUNT)
        except Exception:
            print("Failed to find systemuser by internalemailaddress:")
            traceback.print_exc()
            return

        accounts, job_to_account = load_accounts_with_jobs(dynamics_session)
        contacts_map = load_all_contacts_by_account(dynamics_session)
        jobs = load_all_jobs(dynamics_session)

        # Preload already-contacted contacts if new-only mode
        contacted_contacts = preload_contacted_contacts(dynamics_session) if new_only else set()
    except Exception:
        print("Failed to load data from Dynamics:")
        traceback.print_exc()
        return

    outlook = Dispatch("Outlook.Application")
    target_folder = get_or_create_custom_folder(outlook, CUSTOM_FOLDER_NAME)

    staged_count = 0
    skipped_no_email = 0
    skipped_not_today = 0
    skipped_already_contacted = 0
    missing_attachments = 0

    # iterate jobs and use precomputed attachment paths per lead type
    for job in jobs:
        job_id = job.get("cr21a_jobpostingid")
        account = job_to_account.get(job_id, {"accountid": None, "name": "Unknown"})
        acc_id = account.get("accountid")
        contacts = contacts_map.get(acc_id, [])

        print(f"\nProcessing job: {job.get('cr21a_jobtitle')} at {account.get('name')}")

        for contact in contacts:
            contact_id = contact.get("contactid")
            email_raw = contact.get("emailaddress1")
            recipient = normalize_email(email_raw)

            if not recipient:
                print("Skipping contact with no email")
                skipped_no_email += 1
                continue

            # --- NEW-ONLY FILTER: skip contacts already emailed ---
            if new_only and contact_id in contacted_contacts:
                print(f"Skipping contact {contact_id} (already contacted)")
                skipped_already_contacted += 1
                continue

            # --- NEW FILTER: only contacts created today (UTC date) ---
            createdon_str = contact.get("createdon")
            if not createdon_str:
                print(f"Skipping contact {contact_id} (no createdon)")
                skipped_not_today += 1
                continue

            try:
                # Dynamics typically returns ISO 8601 with Z; normalize to aware datetime
                createdon_dt = datetime.fromisoformat(createdon_str.replace("Z", "+00:00"))
            except Exception:
                print(f"Skipping contact {contact_id} (unable to parse createdon: {createdon_str})")
                skipped_not_today += 1
                continue

            if createdon_dt.date() < cutoff_date:
                print(
                    f"Skipping contact {contact_id} created before cutoff "
                    f"(createdon={createdon_dt.date()}, cutoff={cutoff_date})"
                )
                skipped_not_today += 1
                continue
            # --- END NEW FILTER ---

            leadtype = contact.get("cr21a_leadtype", "")
            attachments = get_attachments_cached(leadtype)
            subject, body = build_email_body(contact, job, account, leadtype)

            if preview:
                preview_email(contact, job, account, subject, body, attachments)
                # In preview mode, don't actually stage the message
                continue

            try:
                stage_email(
                    outlook,
                    contact,
                    job,
                    account,
                    subject,
                    body,
                    attachments,
                    target_folder,
                    dynamics_session,
                    sender_systemuser_id
                )
                staged_count += 1
            except Exception:
                print("Failed to stage email for contact:")
                traceback.print_exc()

            for a in attachments:
                if not os.path.exists(a):
                    missing_attachments += 1

    print("\nSummary")
    print(f"- Staged emails: {staged_count}")
    print(f"- Contacts skipped (no email): {skipped_no_email}")
    print(f"- Contacts skipped (createdon before {cutoff_date}): {skipped_not_today}")
    if new_only:
        print(f"- Contacts skipped (already contacted): {skipped_already_contacted}")
    print(f"- Missing attachments: {missing_attachments}")

    print(f"\nAll eligible emails (contacts created today, {cutoff_date}) staged and logged to Dynamics as Email activities.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage emails and log them to Dynamics")
    parser.add_argument("--preview", action="store_true", help="Print email previews to stdout (no staging)")
    parser.add_argument("--new-only", action="store_true", dest="new_only",
                        help="Only stage emails to contacts not yet contacted (no existing outgoing email activity)")
    args = parser.parse_args()
    # Allow environment override as well
    preview_env = os.getenv("PREVIEW_EMAILS", "").strip().lower() in ("1", "true", "yes")
    main(preview=(args.preview or preview_env), new_only=args.new_only)
