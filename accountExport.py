import glob
import os
import pandas as pd
from datetime import datetime

accounts_export = []

def log_account_for_export(account_name, account_id, website=None):
    accounts_export.append({
        "Company Name": account_name,
        "Account Id": account_id,
        "Website": website or ""
    })

def export_accounts():
    if not accounts_export:
        print("ℹ️ No accounts to export.")
        return
    
    # Deduplicate within current run
    df_new = pd.DataFrame(accounts_export).drop_duplicates(subset=["Account Id"])
    
    # Resolve target directory relative to script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    target_dir = os.path.join(base_dir, "Data", "Dynamics Imports")
    os.makedirs(target_dir, exist_ok=True)
    
    # Load all previous exports
    existing_files = glob.glob(os.path.join(target_dir, "Jake_Dynamics_Acc_Import_*.csv"))
    if existing_files:
        df_existing = pd.concat([pd.read_csv(f) for f in existing_files], ignore_index=True)
        df_existing = df_existing.drop_duplicates(subset=["Account Id"])
        
        # Filter out already exported accounts
        df_new = df_new[~df_new["Account Id"].isin(df_existing["Account Id"])]
    
    if df_new.empty:
        print("ℹ️ No new accounts to export (all already exported previously).")
        return
    
    # Build unique filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"Jake_Dynamics_Acc_Import_{timestamp}.csv"
    filepath = os.path.join(target_dir, filename)
    
    # Write only new accounts
    df_new.to_csv(filepath, index=False)
    
    print(f"✅ Accounts export written: {filepath}")
    print(f"📊 {len(df_new)} new accounts exported")