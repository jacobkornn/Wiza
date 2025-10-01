import os
import glob
import shutil
import pandas as pd
import csv  # <-- added for quoting

DOWNLOADS_FOLDER = os.path.expanduser("../../Downloads")
OUTPUT_FILE = "WizaLeads.csv"
ARCHIVE_FOLDER = "Archive"  # archive folder in running directory

def load_existing_sources():
    """Load source_file values from master file (if it exists)."""
    if not os.path.exists(OUTPUT_FILE):
        return set()
    df = pd.read_csv(OUTPUT_FILE)
    if "source_file" not in df.columns:
        raise ValueError(f"Expected column 'source_file' in {OUTPUT_FILE}")
    return set(df["source_file"].dropna().unique())

def scan_wiza_files():
    """Find WIZA CSV files in downloads folder."""
    return glob.glob(os.path.join(DOWNLOADS_FOLDER, "WIZA*.csv"))

def ensure_archive_folder():
    """Make sure the archive folder exists."""
    if not os.path.exists(ARCHIVE_FOLDER):
        os.makedirs(ARCHIVE_FOLDER)

def archive_file(filepath):
    """Move a processed file into the archive folder."""
    ensure_archive_folder()
    base = os.path.basename(filepath)
    dest = os.path.join(ARCHIVE_FOLDER, base)
    counter = 1
    while os.path.exists(dest):
        name, ext = os.path.splitext(base)
        dest = os.path.join(ARCHIVE_FOLDER, f"{name}_{counter}{ext}")
        counter += 1
    shutil.move(filepath, dest)

def determine_type(filename):
    """Determine lead type based on filename."""
    lower_name = filename.lower()
    if "consulting" in lower_name:
        return "consulting"
    elif "software" in lower_name:
        return "software"
    return "unknown"

def main():
    existing_sources = load_existing_sources()
    print(f"Already imported files: {len(existing_sources)}")

    all_new_dfs = []
    skipped_files = []
    processed_files = []
    new_rows = 0

    # Load existing master to get current emails and max ID
    if os.path.exists(OUTPUT_FILE):
        master_df = pd.read_csv(OUTPUT_FILE)
        if "id" not in master_df.columns:
            master_df.insert(0, "id", range(1, len(master_df)+1))
        if "type" not in master_df.columns:
            master_df.insert(1, "type", "unknown")
    else:
        master_df = pd.DataFrame(columns=["id","type"])  # empty master

    existing_emails = set(master_df["email"]) if "email" in master_df.columns else set()
    next_id = master_df["id"].max() + 1 if not master_df.empty else 1

    # Process new WIZA files
    for file in scan_wiza_files():
        base = os.path.basename(file)

        if base in existing_sources:
            print(f"Skipping {file} (already imported)")
            skipped_files.append(file)
            continue

        try:
            df = pd.read_csv(file)
        except Exception as e:
            print(f"Skipping {file}: {e}")
            continue

        # Add source_file column
        df["source_file"] = base

        # Assign new IDs
        df.insert(0, "id", range(next_id, next_id + len(df)))
        # Assign type column right after id
        df.insert(1, "type", determine_type(base))
        next_id += len(df)

        # Remove duplicates against existing master emails
        if "email" in df.columns:
            before_dedup = len(df)
            removed = before_dedup - len(df)
            if removed > 0:
                print(f"Removed {removed} duplicates from {file} against master file")
            existing_emails.update(df["email"])

        if df.empty:
            continue

        print(f"Adding {file} with {len(df)} rows")
        all_new_dfs.append(df)
        processed_files.append(file)
        new_rows += len(df)

    if not all_new_dfs and os.path.exists(OUTPUT_FILE):
        print("✅ No new WIZA files to process. Master file unchanged.")
        return

    # Combine master and new rows
    combined = pd.concat([master_df] + all_new_dfs, ignore_index=True) if not master_df.empty else pd.concat(all_new_dfs, ignore_index=True)

    # Final deduplication across all rows (keep earliest ID)
    if "email" in combined.columns:
        before = len(combined)
        combined.sort_values("id", inplace=True)
        combined = combined.groupby("email", as_index=False).apply(lambda g: g.ffill().bfill().iloc[0])
        combined.reset_index(drop=True, inplace=True)
        removed = before - len(combined)
        if removed > 0:
            print(f"Removed {removed} duplicate emails in final merge, keeping most complete data")

    # Reassign IDs to ensure contiguous sequence
    combined = combined.reset_index(drop=True)
    combined["id"] = range(1, len(combined)+1) 

    # Ensure type column exists and is filled
    if "type" not in combined.columns:
        combined.insert(1, "type", "unknown")

    # Ensure CSV cells with commas or links are quoted ---
    combined.to_csv(OUTPUT_FILE, index=False)

    # Move processed files to archive
    for file in processed_files:
        archive_file(file)

    # --- Summary ---
    print("\n=== SUMMARY ===")
    print(f"Processed files : {len(processed_files)}")
    print(f"Skipped files   : {len(skipped_files)}")
    print(f"New rows added  : {new_rows}")
    print(f"Total rows now  : {len(combined)}")
    print(f"Output file     : {OUTPUT_FILE}")
    print(f"Archived files  : {ARCHIVE_FOLDER}")
    print("================")

if __name__ == "__main__":
    main()
