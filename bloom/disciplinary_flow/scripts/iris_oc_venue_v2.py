import csv
import sys
import tarfile
import json
import pandas as pd
from io import TextIOWrapper
import time

# Increase CSV field size limit to maximum
csv.field_size_limit(int(sys.maxsize))

# Start timer
start_time = time.time()

#step 1: import iris_oc_index as dataframe
print("Step 1: Importing iris_oc_index...")
iris_oc_index = pd.read_csv("iris_in_oc_index.csv")
print(f"✓ Step 1 complete. Loaded {len(iris_oc_index)} rows\n")

#step 1.1: add "flow" column
print("Step 1.1: Adding flow column...")
def classify_flow(row):
    if row["is_citing_iris"] and row["is_cited_iris"]:
        return "Internal"
    elif row["is_citing_iris"] and not row["is_cited_iris"]:
        return "Outgoing"
    elif not row["is_citing_iris"] and row["is_cited_iris"]:
        return "Incoming"

iris_oc_index["flow"] = iris_oc_index.apply(classify_flow, axis=1)
print("✓ Step 1.1 complete\n")

#step 2: get unique OMIDs to look up
print("Step 2: Creating set of unique OMIDs to match...")
citing_omids_set = set(iris_oc_index["citing"].unique())
cited_omids_set = set(iris_oc_index["cited"].unique())
all_omids_needed = citing_omids_set | cited_omids_set
print(f"✓ Step 2 complete. Need to find {len(all_omids_needed)} unique OMIDs\n")

#step 3: extract venue data directly from tar.gz data dump
print("Step 3: Extracting venue data from OpenCitations data dump...\n")
archive_path = "/Users/regina/OpenSciTations/oc_meta_data_2025-06-06.tar.gz" #change to local file path for data dump tar.gz file

def extract_omid(id_string):
    """Extract omid from id string"""
    if not id_string:
        return None
    for part in id_string.split():
        if part.startswith("omid:"):
            return part
    return None

omids_with_venue = {}  # Dictionary to store {omid: full_venue_string}
omids_without_venue = set()  # Track OMIDs found but with no venue data
rows_processed = 0
files_processed = 0
# MAX_FILES = 10  ##use this if you want to run with just a small sample of the data dump and also uncomment if statement below

with tarfile.open(archive_path, 'r:gz') as archive:
    for member in archive.getmembers():
        # if files_processed >= MAX_FILES:
        #     break
            
        if member.isfile() and member.name.endswith('.csv'):
            files_processed += 1
            print(f"Processing file {files_processed}: {member.name}")
            
            csv_file = archive.extractfile(member)
            reader = csv.DictReader(TextIOWrapper(csv_file, encoding='utf-8', errors='ignore'))
            
            batch_count = 0
            try:
                for row in reader:
                    rows_processed += 1
                    omid = extract_omid(row.get("id", ""))
                    
                    if omid and omid in all_omids_needed:
                        venue = row.get("venue", "")
                        # Only store if venue data exists and is not empty
                        if venue and venue.strip():
                            omids_with_venue[omid] = venue
                        else:
                            omids_without_venue.add(omid)
                    
                    batch_count += 1
                
                print(f"  ✓ File complete. Rows: {batch_count}\n")
                
            except Exception as e:
                print(f"  ✗ Error processing file: {str(e)}")
                print(f"  → Skipping this file and continuing...\n")
                continue

print(f"✓ Step 3 complete. Found {len(omids_with_venue)} matches with venue data")
print(f"  Found {len(omids_without_venue)} OMIDs without venue data\n")

# Save venue_data to external file
with open('omids_with_venue.json', 'w') as f:
    json.dump(omids_with_venue, f, indent=2)
print("✓ Saved omids_with_venue to omids_with_venue.json\n")

# Save OMIDs without venue data
with open('omids_without_venue.json', 'w') as f:
    json.dump(list(omids_without_venue), f, indent=2)
print(f"✓ Saved {len(omids_without_venue)} OMIDs without venue data to omids_without_venue.json\n")

#step 4: add venue data back into iris_oc_index dataframe
print("Step 4: Merging venue data with citations...")

iris_oc_index["citing_venue"] = iris_oc_index["citing"].map(omids_with_venue)
iris_oc_index["cited_venue"] = iris_oc_index["cited"].map(omids_with_venue)

# Matched rows: BOTH citing_venue and cited_venue are present
matched_rows = iris_oc_index[(iris_oc_index["citing_venue"].notna()) & (iris_oc_index["cited_venue"].notna())]
matched_rows = matched_rows.reindex(columns=['id', 'citing', 'citing_venue', 'cited', 'cited_venue', 'creation', 'is_citing_iris', 'is_cited_iris', 'flow'])
matched_rows.to_csv('iris_oc_venues_matched.csv', index=False)

print(f"✓ Saved {len(matched_rows)} rows with venue data to iris_oc_venues_matched.csv\n")

#step 5: save rows with missing venue data
print("Step 5: Saving rows with missing venue data...")

# No-match rows: Either or both venues are missing
no_match_rows = iris_oc_index[(iris_oc_index["citing_venue"].isna()) | (iris_oc_index["cited_venue"].isna())]
no_match_rows = no_match_rows.reindex(columns=['id', 'citing', 'cited', 'creation', 'is_citing_iris', 'is_cited_iris', 'flow'])
no_match_rows.to_csv('iris_oc_venues_no_match.csv', index=False)

print(f"✓ Saved {len(no_match_rows)} rows with missing venue data to iris_oc_venues_no_match.csv\n")

# Calculate and print elapsed time
end_time = time.time()
elapsed_time = end_time - start_time
minutes = int(elapsed_time // 60)
seconds = int(elapsed_time % 60)

print("✓✓✓ All steps complete! ✓✓✓")
print(f"\nTotal execution time: {minutes}m {seconds}s")

# Step 6: Compare row counts
print("\n" + "="*60)
print("SUMMARY - Row Count Comparison:")
print("="*60)
total_original = len(iris_oc_index)
total_matched = len(matched_rows)
total_no_match = len(no_match_rows)
total_identified = total_matched + total_no_match
total_skipped = total_original - total_identified

print(f"Original iris_oc_index rows:      {total_original}")
print(f"Matched rows (with venues):       {total_matched} ({total_matched/total_original*100:.2f}%)")
print(f"No-match rows (no venues):        {total_no_match} ({total_no_match/total_original*100:.2f}%)")
print(f"Total identified in tarfile:      {total_identified} ({total_identified/total_original*100:.2f}%)")
print(f"Skipped/Not found:                {total_skipped} ({total_skipped/total_original*100:.2f}%)")
print("="*60)

# Save summary to txt file
with open('processing_summary.txt', 'w') as f:
    f.write("="*60 + "\n")
    f.write("SUMMARY - Row Count Comparison\n")
    f.write("="*60 + "\n\n")
    f.write(f"Original iris_oc_index rows:      {total_original}\n")
    f.write(f"Matched rows (with venues):       {total_matched} ({total_matched/total_original*100:.2f}%)\n")
    f.write(f"No-match rows (no venues):        {total_no_match} ({total_no_match/total_original*100:.2f}%)\n")
    f.write(f"Total identified in tarfile:      {total_identified} ({total_identified/total_original*100:.2f}%)\n")
    f.write(f"Skipped/Not found:                {total_skipped} ({total_skipped/total_original*100:.2f}%)\n")
    f.write("="*60 + "\n\n")
    f.write(f"Execution time: {minutes}m {seconds}s\n")

print("\n✓ Saved summary to processing_summary.txt")