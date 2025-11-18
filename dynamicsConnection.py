import os
import requests
import pandas as pd
from dotenv import load_dotenv
from msal import ConfidentialClientApplication

load_dotenv()

def get_dynamics_token():
    app = ConfidentialClientApplication(
        client_id=os.getenv("DYNAMICS_CLIENT_ID"),
        client_credential=os.getenv("DYNAMICS_CLIENT_SECRET"),
        authority=f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}"
    )
    token = app.acquire_token_for_client(scopes=[f"{os.getenv('DYNAMICS_ORG_URL')}/.default"])
    if "access_token" not in token:
        raise RuntimeError(f"Token request failed: {token}")
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
            "first_name": c.get("firstname", ""),
            "last_name": c.get("lastname", ""),
            "email": c.get("emailaddress1", ""),
            "company": c.get("company", ""),
            "title": c.get("jobtitle", ""),
            "cr21a-leadtype": c.get("cr21a_leadtype", ""),
            "leadId": c.get("contactid")
        })
    return pd.DataFrame(leads)

def main():
    try:
        df = load_leads_from_dynamics()
        print(f"Retrieved {len(df)} contacts from Dynamics\n")

        if not df.empty:
            # Print each contact on one line
            for _, row in df.iterrows():
                print(
                    f"{row['first_name']} {row['last_name']} | "
                    f"{row['email']} | "
                    f"{row['company']} | "
                    f"{row['title']} | "
                    f"{row['cr21a-leadtype']} | "
                    f"{row['leadId']}"
                )
        else:
            print("No contacts returned from Dynamics.\n")

    except Exception as e:
        print(f"Error retrieving contacts: {e}\n")

if __name__ == "__main__":
    main()