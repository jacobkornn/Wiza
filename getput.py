import requests
import os
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

# --- Step 1: Grab all contacts with their accounts ---
print("\n📥 Fetching all contacts and their accounts...")

contacts_url = (
    f"{DYNAMICS_API}/contacts"
    f"?$select=fullname"
    f"&$expand=parentcustomerid_account($select=name,accountid)"
)

res = requests.get(contacts_url, headers=AUTH_HEADER)
print("GET status:", res.status_code)

if res.status_code != 200:
    print("❌ Error fetching contacts:", res.text)
else:
    data = res.json()
    contacts = data.get("value", [])
    print(f"✅ Retrieved {len(contacts)} contacts\n")

    # --- Step 2: Print each contact and its account ---
    for c in contacts:
        fullname = c.get("fullname", "(no name)")
        account = c.get("parentcustomerid_account", {})
        account_name = account.get("name", "(no account)")
        account_id = account.get("accountid", "")
        print(f"👤 {fullname} → 🏢 {account_name} ({account_id})")