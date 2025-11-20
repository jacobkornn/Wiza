import pandas as pd
import csv  # needed for quoting

INPUT_FILE = "WizaLeads.csv"
OUTPUT_FILE = "WizaCompanies.csv"  # renamed

# Load leads
df = pd.read_csv(INPUT_FILE)

# Check required columns
required_cols = ["id", "company", "company_domain", "company_linkedin", "company_description"]
missing = [col for col in required_cols if col not in df.columns]
if missing:
    raise ValueError(f"Missing columns in {INPUT_FILE}: {missing}")

# Select only relevant columns + leadId
companies_df = df[["id", "company", "company_domain", "company_linkedin", "company_description"]].copy()
companies_df = companies_df.rename(columns={"id": "leadId"})

# --- Minimal change: make company_domain a full URL for clickable links ---
companies_df["company_domain"] = "https://" + companies_df["company_domain"].astype(str)

# Drop duplicate companies (keep first)
before = len(companies_df)
companies_df = companies_df.drop_duplicates(subset=["company"], keep="first")
after = len(companies_df)

print(f"Removed {before - after} duplicate companies. Final unique companies: {after}")

# --- Minimal change: ensure CSV cells with commas are quoted ---
companies_df.to_csv(OUTPUT_FILE, index=False, quoting=csv.QUOTE_ALL)
print(f"Created {OUTPUT_FILE}")
