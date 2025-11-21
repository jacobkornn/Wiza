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

token = get_dynamics_token()
headers = {"Authorization": f"Bearer {token}"}
url = f"{os.getenv('DYNAMICS_ORG_URL')}/api/data/v9.2/accounts?$top=1&$expand=*"
resp = requests.get(url, headers=headers)
print(resp.json())