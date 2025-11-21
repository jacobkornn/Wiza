import os
import re
import requests
import pandas as pd
import traceback
from win32com.client import Dispatch
from dotenv import load_dotenv
from msal import ConfidentialClientApplication

load_dotenv()

# ----------------- CONFIG ----------------- #
OUTLOOK_ACCOUNT = "jake.korn@theboxk.com"
CUSTOM_FOLDER_NAME = "JakeJobs Outbound"

# ----------------- Templates ----------------- #
SALES_TEMPLATE = """<html><body>
<p>Hi {firstname},</p>
<p>I applied for {account_name}’s {cr21a_jobtitle} position and wanted to reach out directly.
With a CS background and startup experience applying machine learning to sales and marketing,
I bring technical depth plus strong communication skills.
Given your role as {contact_jobtitle}, I’d love to connect and learn more about {account_name}’s sales approach.</p>
<p>Best,<br>{your_full_name}</p>
<p><br>(425) 354-0440<br>
<a href="https://www.linkedin.com/in/jacob-korn-3aa792248/">My LinkedIn</a></p>
</body></html>"""

ENGINEERING_TEMPLATE = """<html><body>
<p>Hi {firstname},</p>
<p>I noticed {account_name} has an opening for {cr21a_jobtitle}.
With my engineering background and experience building scalable systems,
I bring technical depth and problem-solving skills.
Given your role as {contact_jobtitle}, I’d love to connect and learn more about {account_name}’s engineering priorities.</p>
<p>Best,<br>{your_full_name}</p>
<p><br>(425) 354-0440<br>
<a href="https://www.linkedin.com/in/jacob-korn-3aa792248/">My LinkedIn</a></p>
</body></html>"""

# ----------------- Helper Functions ----------------- #
def normalize_email(addr):
    if pd.isna(addr):
        return ""
    e = str(addr).strip()
    e = re.sub(r'^mailto:', '', e, flags=re.I)
    return e.strip().strip('"').lower()

def strip_html_tags(text):
    return re.sub(r'<[^>]+>', '', text)

# ----------------- Dynamics Integration ----------------- #
def get_dynamics_token():
    print("Getting Dynamics token...")
    app = ConfidentialClientApplication(
        client_id=os.getenv("DYNAMICS_CLIENT_ID"),
        client_credential=os.getenv("DYNAMICS_CLIENT_SECRET"),
        authority=f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}"
    )
    token = app.acquire_token_for_client(scopes=[f"{os.getenv('DYNAMICS_ORG_URL')}/.default"])
    if "access_token" not in token:
        raise Exception(f"Failed to get token: {token}")
    print("Got Dynamics token")
    return token["access_token"]

def load_accounts_with_jobs():
    print("Loading accounts with expanded job postings...")
    token = get_dynamics_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = (
        f"{os.getenv('DYNAMICS_ORG_URL')}/api/data/v9.2/accounts"
        f"?$select=accountid,name"
        f"&$expand=cr21a_Account_to_JobPosting($select=cr21a_jobpostingid,cr21a_jobtitle)"
    )
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    accounts = resp.json()["value"]

    account_map = {a["accountid"]: a for a in accounts}
    job_to_account = {}
    for a in accounts:
        jobs = a.get("cr21a_Account_to_JobPosting", []) or []
        for j in jobs:
            job_to_account[j["cr21a_jobpostingid"]] = a

    print(f"Loaded {len(account_map)} accounts; mapped {len(job_to_account)} jobs to accounts")
    return account_map, job_to_account

def load_all_contacts_by_account():
    print("Loading all contacts...")
    token = get_dynamics_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = (
        f"{os.getenv('DYNAMICS_ORG_URL')}/api/data/v9.2/contacts"
        f"?$select=contactid,firstname,lastname,emailaddress1,jobtitle,cr21a_leadtype,_parentcustomerid_value"
    )
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    contacts = resp.json()["value"]

    contact_map = {}
    for c in contacts:
        acc_id = c.get("_parentcustomerid_value")
        if acc_id:
            contact_map.setdefault(acc_id, []).append(c)

    total_contacts = sum(len(v) for v in contact_map.values())
    print(f"Indexed {total_contacts} contacts across {len(contact_map)} accounts")
    return contact_map

def load_all_jobs():
    print("Loading all job postings...")
    token = get_dynamics_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{os.getenv('DYNAMICS_ORG_URL')}/api/data/v9.2/cr21a_jobpostings?$select=cr21a_jobpostingid,cr21a_jobtitle"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    jobs = resp.json()["value"]
    print(f"Loaded {len(jobs)} job postings")
    return jobs

def log_email_to_dynamics(contact_id, jobposting_id, subject, body):
    print(f"Logging email to Dynamics for contact {contact_id}, job {jobposting_id}...")
    token = get_dynamics_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json;odata.metadata=minimal"
    }
    payload = {
        "subject": subject,
        "description": body,
        "directioncode": True,  # outgoing
        "regardingobjectid_contact@odata.bind": f"/contacts({contact_id})",
        "regardingobjectid_cr21a_jobposting@odata.bind": f"/cr21a_jobpostings({jobposting_id})"
    }
    url = f"{os.getenv('DYNAMICS_ORG_URL')}/api/data/v9.2/emails"
    resp = requests.post(url, headers=headers, json=payload)
    if not resp.ok:
        print(f"Error logging email {resp.status_code}: {resp.text}")
    else:
        print("Logged email successfully")

# ----------------- Outlook Integration ----------------- #
def get_or_create_custom_folder(outlook, folder_name):
    namespace = outlook.GetNamespace("MAPI")
    root = namespace.Folders[OUTLOOK_ACCOUNT]
    try:
        target_folder = root.Folders.Item(folder_name)
    except:
        target_folder = root.Folders.Add(folder_name)
    return target_folder

def select_documents(leadtype):
    leadtype = str(leadtype).strip().lower()
    if leadtype == "sales":
        base_dir = "Data/Sales"
    elif leadtype == "engineering":
        base_dir = "Data/Software"
    else:
        base_dir = "Data/Software"
    resume = os.path.join(base_dir, "Jacob_Korn_Resume.pdf")
    cover = os.path.join(base_dir, "Jacob_Korn_CoverLetter.pdf")
    return [resume, cover]

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

def stage_email(outlook, contact, job, account, subject, body, attachments, target_folder):
    try:
        print(f"Staging email to {contact.get('emailaddress1')}...")
        mail = outlook.CreateItem(0)
        mail.To = normalize_email(contact.get("emailaddress1"))
        mail.Subject = subject
        mail.HTMLBody = body
        for attachment in attachments:
            if os.path.exists(attachment):
                mail.Attachments.Add(os.path.abspath(attachment))
        mail.Save()
        mail.Move(target_folder)
        log_email_to_dynamics(
            contact_id=contact["contactid"],
            jobposting_id=job["cr21a_jobpostingid"],
            subject=subject,
            body=body
        )
        print("Staged email successfully")
    except Exception:
        print("Error staging email:")
        traceback.print_exc()
        raise

# ----------------- Main Workflow ----------------- #
def main():
    print("Starting Dynamics email staging app...")

    try:
        accounts, job_to_account = load_accounts_with_jobs()
        contacts_map = load_all_contacts_by_account()
        jobs = load_all_jobs()
    except Exception:
        print("Failed to load data from Dynamics:")
        traceback.print_exc()
        return

    outlook = Dispatch("Outlook.Application")
    target_folder = get_or_create_custom_folder(outlook, CUSTOM_FOLDER_NAME)

    staged_count = 0
    skipped_no_email = 0
    missing_attachments = 0

    for job in jobs:
        job_id = job.get("cr21a_jobpostingid")
        account = job_to_account.get(job_id, {"accountid": None, "name": "Unknown"})
        acc_id = account.get("accountid")
        contacts = contacts_map.get(acc_id, [])

        print(f"\nProcessing job: {job.get('cr21a_jobtitle')} at {account.get('name')}")

        for contact in contacts:
            leadtype = contact.get("cr21a_leadtype", "")
            attachments = select_documents(leadtype)
            subject, body = build_email_body(contact, job, account, leadtype)
            preview_email(contact, job, account, subject, body, attachments)

            recipient = normalize_email(contact.get("emailaddress1"))
            if not recipient:
                print("Skipping contact with no email")
                skipped_no_email += 1
                continue

            try:
                stage_email(outlook, contact, job, account, subject, body, attachments, target_folder)
                staged_count += 1
            except Exception:
                print("Failed to stage email:")
                traceback.print_exc()

            for a in attachments:
                if not os.path.exists(a):
                    missing_attachments += 1

    print("\nSummary")
    print(f"- Staged emails: {staged_count}")
    print(f"- Contacts skipped (no email): {skipped_no_email}")
    print(f"- Missing attachments: {missing_attachments}")
    print("\nAll emails staged and logged to Dynamics as Sent.")

if __name__ == "__main__":
    main()    