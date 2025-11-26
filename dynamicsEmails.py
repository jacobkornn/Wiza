import os
import re
import time
import argparse
import requests
import traceback
import pandas as pd
from datetime import datetime, timedelta, timezone
from win32com.client import Dispatch
from dotenv import load_dotenv
from msal import ConfidentialClientApplication

load_dotenv()

# ----------------- CONFIG ----------------- #
OUTLOOK_ACCOUNT = os.getenv("OUTLOOK_ACCOUNT", "jake.korn@theboxk.com")
CUSTOM_FOLDER_NAME = os.getenv("CUSTOM_FOLDER_NAME", "JakeJobs Outbound")
# How many days to "cool down" a contact before emailing them again
CONTACT_COOLDOWN_DAYS = int(os.getenv("CONTACT_COOLDOWN_DAYS", "7"))

# ----------------- Templates ----------------- #
SALES_TEMPLATE = """<html><body>
<p>Hi {firstname},</p>
<p>I recently applied for the {cr21a_jobtitle} position at {account_name} and wanted to reach out directly.
With a CS background and startup experience applying machine learning to sales and marketing,
I bring technical depth plus strong communication skills.
Given your role as {contact_jobtitle}, I’d love to connect and learn more about {account_name}’s sales approach.</p>
<p>Best,<br>{your_full_name}</p>
<p><br>(425) 354-0440<br>
<a href="https://www.linkedin.com/in/jacob-korn-3aa792248/">My LinkedIn</a></p>
</body></html>"""

ENGINEERING_TEMPLATE = """<html><body>
<p>Hi {firstname},</p>
<p>I recently applied for the {cr21a_jobtitle} position at {account_name} and wanted to reach out directly.
With my CS background and startup experience building and applying machine learning to CRM data workflows,
I bring technical depth and complex problem-solving skills.
Given your role as {contact_jobtitle}, I’d love to connect and learn more about {account_name}’s engineering practices.</p>
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

# ----------------- Dynamics Auth (cached, reusable) ----------------- #
_msal_app = None
_cached_token = None  # dict with access_token, expires_in, ext_expires_in...
_token_acquired_at = 0.0

def init_msal():
    global _msal_app
    if _msal_app is None:
        _msal_app = ConfidentialClientApplication(
            client_id=os.getenv("DYNAMICS_CLIENT_ID"),
            client_credential=os.getenv("DYNAMICS_CLIENT_SECRET"),
            authority=f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}"
        )
    return _msal_app

def get_dynamics_token(force_refresh=False):
    """
    Return a valid access token. Caches token in-memory and refreshes
    if expired (uses expires_in from MSAL response).
    """
    global _cached_token, _token_acquired_at
    app = init_msal()

    if not force_refresh and _cached_token and "access_token" in _cached_token:
        expires_in = int(_cached_token.get("expires_in", 0))
        # refresh 60 seconds before expiry to be safe
        if time.time() < _token_acquired_at + expires_in - 60:
            return _cached_token["access_token"]

    token = app.acquire_token_for_client(scopes=[f"{os.getenv('DYNAMICS_ORG_URL')}/.default"])
    if "access_token" not in token:
        raise Exception(f"Failed to get token: {token}")
    _cached_token = token
    _token_acquired_at = time.time()
    return token["access_token"]

def build_dynamics_session():
    """
    Returns a requests.Session with Authorization header attached.
    Use this session for all Dynamics API calls.
    """
    token = get_dynamics_token()
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json;odata.metadata=minimal",
        # include lookup logical name annotations
        "Prefer": 'odata.include-annotations="*"'
    })
    return s

# ----------------- Dynamics data loaders (rely on session) ----------------- #
def load_accounts_with_jobs(session):
    print("Loading accounts with expanded job postings...")
    url = (
        f"{os.getenv('DYNAMICS_ORG_URL')}/api/data/v9.2/accounts"
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
    print("Loading all contacts...")
    # Select important fields only
    url = (
        f"{os.getenv('DYNAMICS_ORG_URL')}/api/data/v9.2/contacts"
        f"?$select=contactid,firstname,lastname,fullname,emailaddress1,jobtitle,cr21a_leadtype,_parentcustomerid_value"
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
    url = f"{os.getenv('DYNAMICS_ORG_URL')}/api/data/v9.2/cr21a_jobpostings?$select=cr21a_jobpostingid,cr21a_jobtitle"
    resp = session.get(url)
    resp.raise_for_status()
    jobs = resp.json().get("value", [])
    print(f"Loaded {len(jobs)} job postings")
    return jobs

# ----------------- Recent-email checker ----------------- #
def load_recently_emailed_contact_ids(session, days=7):
    """
    Return a set of contact IDs that have been emailed (as To recipients)
    within the last `days` days.
    """
    org = os.getenv("DYNAMICS_ORG_URL").rstrip("/")
    emails_url = f"{org}/api/data/v9.2/emails"

    # Use timezone-aware UTC datetime, formatted as 'YYYY-MM-DDTHH:MM:SSZ'
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"Loading emails sent since {cutoff_iso} to build recent-contact list...")

    # Outgoing emails since cutoff, with expanded activity parties (To recipients)
    params = {
        "$select": "activityid,subject,createdon",
        "$filter": f"createdon ge {cutoff_iso} and directioncode eq true",
        "$expand": (
            "email_activity_parties("
            "$select=_partyid_value,participationtypemask;"
            "$filter=participationtypemask eq 2)"
        ),
        "$top": "500"
    }

    recently_contacted = set()

    while True:
        resp = session.get(emails_url, params=params)
        resp.raise_for_status()
        data = resp.json()
        emails = data.get("value", [])
        print(f"  Retrieved {len(emails)} email(s) page")

        for email in emails:
            parties = email.get("email_activity_parties") or []
            for p in parties:
                # Only contact recipients
                logical_name = p.get("_partyid_value@Microsoft.Dynamics.CRM.lookuplogicalname")
                if logical_name == "contact":
                    cid = p.get("_partyid_value")
                    if cid:
                        recently_contacted.add(cid)

        next_link = data.get("@odata.nextLink")
        if not next_link:
            break

        # nextLink already includes all query info
        emails_url = next_link
        params = None

    print(f"Found {len(recently_contacted)} contacts emailed in the last {days} day(s).")
    return recently_contacted

# ----------------- System user lookup ----------------- #
def find_systemuser_id_by_internal_email(session, email):
    """
    Lookup systemuser by internalemailaddress. Returns systemuserid GUID string.
    Raises Exception if not found.
    """
    if not email:
        raise ValueError("Email must be provided to lookup systemuser")

    org = os.getenv("DYNAMICS_ORG_URL").rstrip('/')
    url = f"{org}/api/data/v9.2/systemusers"
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
        url = f"{os.getenv('DYNAMICS_ORG_URL')}/api/data/v9.2/emails"
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
    subject = f"Application for {template_data['cr21a_jobtitle']} at {template_data['account_name']}"
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
def main(preview=False):
    print("Starting Dynamics email staging app...")

    # Build single Dynamics session for all API traffic (token managed by get_dynamics_token)
    try:
        dynamics_session = build_dynamics_session()

        # Lookup the systemuser id by internalemailaddress (matching OUTLOOK_ACCOUNT)
        try:
            sender_systemuser_id = find_systemuser_id_by_internal_email(dynamics_session, OUTLOOK_ACCOUNT)
        except Exception:
            print("Failed to find systemuser by internalemailaddress:")
            traceback.print_exc()
            return

        # Build "cooldown" list of contacts recently emailed
        recently_contacted_ids = load_recently_emailed_contact_ids(
            dynamics_session,
            days=CONTACT_COOLDOWN_DAYS
        )

        accounts, job_to_account = load_accounts_with_jobs(dynamics_session)
        contacts_map = load_all_contacts_by_account(dynamics_session)
        jobs = load_all_jobs(dynamics_session)
    except Exception:
        print("Failed to load data from Dynamics:")
        traceback.print_exc()
        return

    outlook = Dispatch("Outlook.Application")
    target_folder = get_or_create_custom_folder(outlook, CUSTOM_FOLDER_NAME)

    staged_count = 0
    skipped_no_email = 0
    skipped_recent = 0
    missing_attachments = 0

    # For summary printing of excluded contacts
    excluded_recent_contacts = []

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

            # Skip if this contact was emailed recently
            if contact_id in recently_contacted_ids:
                name = contact.get("fullname") or f"{contact.get('firstname','')} {contact.get('lastname','')}".strip()
                print(
                    f"Skipping contact (recently emailed): {name} "
                    f"<{recipient}> - contacted within last {CONTACT_COOLDOWN_DAYS} day(s)"
                )
                skipped_recent += 1
                excluded_recent_contacts.append((contact_id, name, recipient))
                continue

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
    print(f"- Contacts skipped (recently contacted ≤ {CONTACT_COOLDOWN_DAYS} days): {skipped_recent}")
    print(f"- Missing attachments: {missing_attachments}")

    if excluded_recent_contacts:
        print("\nContacts excluded due to recent email activity:")
        for cid, name, email in excluded_recent_contacts:
            print(f"  - {name} <{email}> ({cid})")

    print("\nAll eligible emails staged and logged to Dynamics as Email activities.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage emails and log them to Dynamics")
    parser.add_argument("--preview", action="store_true", help="Print email previews to stdout (no staging)")
    args = parser.parse_args()
    # Allow environment override as well
    preview_env = os.getenv("PREVIEW_EMAILS", "").strip().lower() in ("1", "true", "yes")
    main(preview=(args.preview or preview_env))
