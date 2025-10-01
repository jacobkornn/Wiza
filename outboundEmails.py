import pandas as pd
import os
import re
from datetime import datetime
from win32com.client import Dispatch
from dotenv import load_dotenv
load_dotenv()  # load EMAILS_SECRET from .env

# CONFIG
CSV_FILE = "WizaLeads.csv"
DATA_FOLDER = "Data"
RESUME_FILE = os.path.join(DATA_FOLDER, "jacobkorn_resume.docx")
SWD_COVERLETTER_FILE = os.path.join(DATA_FOLDER, "jacobkorn_coverletter.docx")
CONSULTING_COVERLETTER_FILE = os.path.join(DATA_FOLDER, "Consulting", "jacobkorn_coverletter.docx")
LOG_FILE = "SentEmails.csv"
FIELDS_USED = ["first_name", "company", "title"]
CUSTOM_FOLDER_NAME = "Wiza Emails - Outbound"

#Software Development, job title data exists
SWD_EMAIL_TEMPLATE_WITHTITLE = """\
<html>
<body>
<p>Hello {first_name},</p>

<p>I hope this email finds you well! I noticed you're working with {company} as a {title}. 
I thought I would reach out to introduce myself and hopefully make your job a little bit easier!</p>

<p>My name is Jacob and I am a Software Developer with expertise in both front-end 
(JavaScript, CSS, HTML) and back-end (C#, Python, SQL) development. I have experience in all stages of the 
software development lifecycle and proficiency in CI/CD practices. I am passionate about delivering 
high-quality software solutions and driven by curiosity in exploring new technologies.</p>

<p>Here is my <a href="https://www.linkedin.com/in/jacob-korn-3aa792248/">LinkedIn</a>. 
Attached are my resume and cover letter as well. Please, don't be afraid to reach out if you come across any open positions at {company} that match my qualifications. 
I am both eager to learn more about the company and to explore new connections!</p>

<p>Wishing you all the best,<br>
Jacob Korn</p>
</body>
</html>
"""
#Software Development, no job title data
SWD_EMAIL_TEMPLATE_NO_TITLE = """\
<html>
<body>
<p>Hello {first_name},</p>

<p>I hope this email finds you well! I noticed you're working at {company}. 
I thought I would reach out to introduce myself and hopefully make your job a little bit easier!</p>

<p>My name is Jacob and I am a Software Developer with expertise in both front-end 
(JavaScript, CSS, HTML) and back-end (C#, Python, SQL) development. I have experience in all stages of the 
software development lifecycle and proficiency in CI/CD practices. I am passionate about delivering 
high-quality software solutions and driven by curiosity in exploring new technologies.</p>

<p>Here is my <a href="https://www.linkedin.com/in/jacob-korn-3aa792248/">LinkedIn</a>. 
Attached are my resume and cover letter as well. Please, don't be afraid to reach out if you come across any open positions at {company} that match my qualifications. 
I am both eager to learn more about the company and to explore new connections!</p>

<p>Wishing you all the best,<br>
Jacob Korn</p>
</body>
</html>
"""
#Consulting, job title data exists
CONSULTING_EMAIL_TEMPLATE_WITHTITLE = """\
<html>
<body>
<p>Hello {first_name},</p>

<p>I hope this email finds you well! I noticed you're working with {company} as a {title}. 
I thought I would reach out to introduce myself and hopefully make your job a little bit easier!</p>

<p>My name is Jacob and I am a Software Developer. I co-founded a startup and have led R&amp;D on AI-driven data enrichment classifiers to optimize CRM targeting 
for digital sales and marketing. My experience spans machine learning and full-stack development — 
Python, C#, JavaScript, HTML, SQL/PostgreSQL, React, Dynamics 365, Power Platform, and Azure — with a focus 
on delivering scalable solutions aligned with broader business goals.</p>

<p>Having built systems end-to-end, I am now excited to shift into consulting: applying the same technical 
depth and product-driven mindset to new industries, where I can translate complex challenges into strategies 
and solutions that drive measurable results.</p>

<p>Here is my <a href="https://www.linkedin.com/in/jacob-korn-3aa792248/">LinkedIn</a>. 
Attached are my resume and cover letter as well. Please, don't be afraid to reach out if you come across any open positions at {company} that match my qualifications. 
I am both eager to learn more about the company and to explore new connections!</p>

<p>Wishing you all the best,<br>
Jacob Korn</p>
</body>
</html>
"""
#Consulting, no job title data
CONSULTING_EMAIL_TEMPLATE_NO_TITLE =  """\
<html>
<body>
<p>Hello {first_name},</p>

<p>I hope this email finds you well! I noticed you're working at {company}. 
I thought I would reach out to introduce myself and hopefully make your job a little bit easier!</p>

<p>My name is Jacob and I am a Software Developer. I co-founded a startup and have led R&amp;D on AI-driven data enrichment classifiers to optimize CRM targeting 
for digital sales and marketing. My experience spans machine learning and full-stack development — 
Python, C#, JavaScript, HTML, SQL/PostgreSQL, React, Dynamics 365, Power Platform, and Azure — with a focus 
on delivering scalable solutions aligned with broader business goals.</p>

<p>Having built systems end-to-end, I am now excited to shift into consulting: applying the same technical 
depth and product-driven mindset to new industries, where I can translate complex challenges into strategies 
and solutions that drive measurable results.</p>

<p>Here is my <a href="https://www.linkedin.com/in/jacob-korn-3aa792248/">LinkedIn</a>. 
Attached are my resume and cover letter as well. Please, don't be afraid to reach out if you come across any open positions at {company} that match my qualifications. 
I am both eager to learn more about the company and to explore new connections!</p>

<p>Wishing you all the best,<br>
Jacob Korn</p>
</body>
</html>
"""

# ----------------- Helper Functions ----------------- #

def load_leads(csv_file):
    return pd.read_csv(csv_file)

def strip_html_tags(text):
    return re.sub(r'<[^>]+>', '', text)

def preview_email(lead):
    lead_data = {k: lead.get(k, "") for k in FIELDS_USED}

    lead_type = lead.get("type", "software").lower()
    title_val = lead_data.get("title")

    # Select template based on type
    if lead_type == "consulting":
        if pd.notna(title_val) and str(title_val).strip() != "":
            email_body = CONSULTING_EMAIL_TEMPLATE_WITHTITLE.format(**lead_data)
        else:
            email_body = CONSULTING_EMAIL_TEMPLATE_NO_TITLE.format(**lead_data)
    elif lead_type == "software":
        if pd.notna(title_val) and str(title_val).strip() != "":
            email_body = SWD_EMAIL_TEMPLATE_WITHTITLE.format(**lead_data)
        else:
            email_body = SWD_EMAIL_TEMPLATE_NO_TITLE.format(**lead_data)
    else:
        if pd.notna(title_val) and str(title_val).strip() != "":
            email_body = SWD_EMAIL_TEMPLATE_WITHTITLE.format(**lead_data)
        else:
            email_body = SWD_EMAIL_TEMPLATE_NO_TITLE.format(**lead_data)

    print("\n--- Lead Data (used in template) ---")
    for k, v in lead_data.items():
        print(f"{k}: {v}")

    print("\n--- Email Preview (console) ---")
    print(strip_html_tags(email_body))

    print("\n--- Attachments ---")
    # Select cover letter based on type
    if lead_type == "consulting":
        cover_file = CONSULTING_COVERLETTER_FILE
    elif lead_type == "software":
        cover_file = SWD_COVERLETTER_FILE
    else:
        cover_file = SWD_COVERLETTER_FILE

    for attachment in [RESUME_FILE, cover_file]:
        if os.path.exists(attachment):
            print(f"{attachment} (will be attached)")
        else:
            print(f"{attachment} (MISSING)")
    print("-" * 40)
    return email_body

def log_email(leadId, email, status, batchId, error_message=""):
    if os.path.exists(LOG_FILE):
        log_df = pd.read_csv(LOG_FILE)
        next_id = log_df['id'].max() + 1
    else:
        log_df = pd.DataFrame(columns=['id', 'batchId', 'leadId', 'email', 'status', 'error_message', 'timestamp'])
        next_id = 1

    new_row = {
        'id': next_id,
        'batchId': batchId,
        'leadId': leadId,
        'email': email,
        'status': status,
        'error_message': error_message,
        'timestamp': datetime.now().isoformat()
    }

    log_df = pd.concat([log_df, pd.DataFrame([new_row])], ignore_index=True)
    log_df = log_df.sort_values('leadId', ascending=True)
    log_df.to_csv(LOG_FILE, index=False)

def get_or_create_custom_folder(outlook, folder_name):
    namespace = outlook.GetNamespace("MAPI")
    root = namespace.Folders['jacob.korn@outlook.com']
    try:
        target_folder = root.Folders.Item(folder_name)
    except:
        target_folder = root.Folders.Add(folder_name)
    return target_folder

# ----------------- Main Workflow ----------------- #

def main():
    secret = os.getenv("EMAILS_SECRET")
    if not secret:
        print("ERROR: EMAILS_SECRET not set in .env")
        return

    user_input = input("Enter secret to confirm staging emails: ")
    if user_input != secret:
        print("Secret incorrect. Exiting without creating drafts.")
        return

    # Determine next batchId
    if os.path.exists(LOG_FILE):
        existing_log = pd.read_csv(LOG_FILE)
        next_batch_id = int(existing_log['batchId'].max()) + 1 if not existing_log.empty else 1
    else:
        next_batch_id = 1

    df = load_leads(CSV_FILE)
    outlook = Dispatch("Outlook.Application")
    target_folder = get_or_create_custom_folder(outlook, CUSTOM_FOLDER_NAME)

    print(f"Loaded {len(df)} leads from {CSV_FILE}\n")
    drafts_to_create = []

    # Preview and prepare emails
    for idx, row in df.iterrows():
        lead_data = row.to_dict()
        email_body = preview_email(lead_data)
        drafts_to_create.append((lead_data, email_body))

    # Stage emails into the custom folder
    for lead_data, email_body in drafts_to_create:
        recipient = lead_data.get("email")
        lead_id = lead_data.get("leadId")
        try:
            if pd.notna(recipient):
                mail = outlook.CreateItem(0)  # olMailItem
                mail.To = recipient
                lead_type = lead_data.get("type", "").lower()  # e.g., "software" or "consulting"
                if lead_type == "software":
                    mail.Subject = f"Inquiry - Software Development at {lead_data.get('company', '')}"
                elif lead_type == "consulting":
                    mail.Subject = f"Inquiry - Consulting at {lead_data.get('company', '')}"
                else:
                    mail.Subject = f"Inquiry at {lead_data.get('company', '')}"
                mail.HTMLBody = email_body
                lead_type = lead_data.get("type", "software").lower()
                if lead_type == "consulting":
                    cover_file = CONSULTING_COVERLETTER_FILE
                elif lead_type == "software":
                    cover_file = SWD_COVERLETTER_FILE
                else:
                    cover_file = SWD_COVERLETTER_FILE
                for attachment in [RESUME_FILE, cover_file]:
                    if os.path.exists(attachment):
                        mail.Attachments.Add(os.path.abspath(attachment))
                mail.Save()  # Save to Drafts temporarily
                mail.Move(target_folder)  # Move to custom folder
                log_email(lead_id, recipient, "DRAFT", batchId=next_batch_id)
            else:
                log_email(lead_id, recipient, "FAILURE", batchId=next_batch_id, error_message="Missing recipient email")
        except Exception as e:
            log_email(lead_id, recipient, "FAILURE", batchId=next_batch_id, error_message=str(e))

    print(f"\nAll emails staged. SentEmails.csv updated with status='DRAFT'.")

if __name__ == "__main__":
    main()
