import os
import re
import pandas as pd
import requests
from win32com.client import Dispatch
from dotenv import load_dotenv
from msal import ConfidentialClientApplication

load_dotenv()

# CONFIG
SOFTWARE_FOLDER = "Data/Software"
SALES_FOLDER = "Data/Sales"

SOFTWARE_RESUME = os.path.join(SOFTWARE_FOLDER, "Jacob_Korn_Resume.pdf")
SALES_RESUME = os.path.join(SALES_FOLDER, "Jacob_Korn_Resume.pdf")
SOFTWARE_COVERLETTER = os.path.join(SOFTWARE_FOLDER, "Jacob_Korn_CoverLetter.pdf")
SALES_COVERLETTER = os.path.join(SALES_FOLDER, "Jacob_Korn_CoverLetter.pdf")

CUSTOM_FOLDER_NAME = "Dynamics Emails - Outbound"
OUTLOOK_ACCOUNT = "jake.korn@theboxk.com" 

FIELDS_USED = ["first_name", "company", "title"]

# ----------------- Template Placeholders ----------------- #
SOFTWARE_TEMPLATE = """<html><body><p>Hello {first_name},</p><p>[Software Placeholder]</p></body></html>"""
SALES_TEMPLATE = """<html><body><p>Hello {first_name},</p><p>[Sales Placeholder]</p></body></html>"""
GENERIC_TEMPLATE = """<html><body><p>Hello {first_name},</p><p>[Generic Placeholder]</p></body></html>"""

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
    app = ConfidentialClientApplication(
        client_id=os.getenv("DYNAMICS_CLIENT_ID"),
        client_credential=os.getenv("DYNAMICS_CLIENT_SECRET"),
        authority=f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}"
    )
    token = app.acquire_token_for_client(scopes=[f"{os.getenv('DYNAMICS_ORG_URL')}/.default"])
    return token["access_token"]

def load_leads_from_dynamics():
    token = get_dynamics_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{os.getenv('DYNAMICS_ORG_URL')}/api/data/v9.2/contacts?$select=contactid,firstname,lastname,emailaddress1,jobtitle,company,cr21a_leadtype"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    contacts = resp.json()["value"]

    leads = []
    for c in contacts:
        leads.append({
            "leadId": c.get("contactid"),
            "first_name": c.get("firstname", ""),
            "company": c.get("company", ""),
            "title": c.get("jobtitle", ""),
            "email": c.get("emailaddress1", ""),
            "cr21a-leadtype": c.get("cr21a_leadtype", "")
        })
    return pd.DataFrame(leads)

def log_email_to_dynamics(contact_id, subject, body):
    token = get_dynamics_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json;odata.metadata=minimal"
    }
    payload = {
        "subject": subject,
        "description": body,
        "regardingobjectid_contact@odata.bind": f"/contacts({contact_id})",
        "statuscode": 2  # Always mark as Sent
    }
    url = f"{os.getenv('DYNAMICS_ORG_URL')}/api/data/v9.2/emails"
    resp = requests.post(url, headers=headers, json=payload)
    if not resp.ok:
        print(f"Error logging email to Dynamics: {resp.text}")

# ----------------- Outlook Integration ----------------- #

def get_or_create_custom_folder(outlook, folder_name):
    namespace = outlook.GetNamespace("MAPI")
    root = namespace.Folders[OUTLOOK_ACCOUNT]   # <-- updated to your account
    try:
        target_folder = root.Folders.Item(folder_name)
    except:
        target_folder = root.Folders.Add(folder_name)
    return target_folder

def preview_email(lead):
    lead_data = {k: lead.get(k, "") for k in FIELDS_USED}

    # Normalize cr21a-leadtype (case-insensitive)
    lead_type = str(lead.get("cr21a-leadtype", "")).strip().lower()

    # Select template + attachments based on cr21a-leadtype
    if lead_type == "software":
        email_body = SOFTWARE_TEMPLATE.format(**lead_data)
        resume_file = SOFTWARE_RESUME
        cover_file = SOFTWARE_COVERLETTER
    elif lead_type == "sales":
        email_body = SALES_TEMPLATE.format(**lead_data)
        resume_file = SALES_RESUME
        cover_file = SALES_COVERLETTER
    else:
        email_body = GENERIC_TEMPLATE.format(**lead_data)
        resume_file = SOFTWARE_RESUME  # default fallback
        cover_file = SOFTWARE_COVERLETTER

    print("\n--- Lead Data ---")
    for k, v in lead_data.items():
        print(f"{k}: {v}")

    print("\n--- Email Preview ---")
    print(strip_html_tags(email_body))

    print("\n--- Attachments ---")
    for attachment in [resume_file, cover_file]:
        if os.path.exists(attachment):
            print(f"{attachment} (will be attached)")
        else:
            print(f"{attachment} (MISSING)")
    print("-" * 40)

    return email_body, [resume_file, cover_file]

# ----------------- Main Workflow ----------------- #

def main():
    secret = os.getenv("EMAILS_SECRET")
    if not secret:
        print("ERROR: EMAILS_SECRET not set in .env")
        return

    user_input = input("Enter secret to confirm staging emails: ")
    if user_input != secret:
        print("Secret incorrect. Exiting.")
        return

    df = load_leads_from_dynamics()
    outlook = Dispatch("Outlook.Application")
    target_folder = get_or_create_custom_folder(outlook, CUSTOM_FOLDER_NAME)

    print(f"Loaded {len(df)} leads from Dynamics\n")

    for _, row in df.iterrows():
        lead_data = row.to_dict()
        recipient = lead_data.get("email")
        if not recipient:
            continue

        email_body, attachments = preview_email(lead_data)

        try:
            mail = outlook.CreateItem(0)
            mail.To = recipient
            mail.Subject = f"Inquiry - {lead_data.get('company', '')}"
            mail.HTMLBody = email_body
            for attachment in attachments:
                if os.path.exists(attachment):
                    mail.Attachments.Add(os.path.abspath(attachment))
            mail.Save()
            mail.Move(target_folder)

            # Always log as Sent
            log_email_to_dynamics(lead_data["leadId"], mail.Subject, email_body)
        except Exception as e:
            print(f"Error staging email: {e}")

    print("\nAll emails staged and logged to Dynamics as Sent.")

if __name__ == "__main__":
    main()