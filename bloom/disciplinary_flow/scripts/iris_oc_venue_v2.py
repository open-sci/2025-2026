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
archive_path = "/Users/regina/output_csv_2026_01_14.tar.gz" #change to local file path for data dump tar.gz file

def extract_omid(id_string):
    """Extract omid from id string"""
    if not id_string:
        return None
    for part in id_string.split():
        if part.startswith("omid:"):
            return part
    return None

venue_data = {}  # Dictionary to store {omid: full_venue_string}
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
                            venue_data[omid] = venue
                        else:
                            omids_without_venue.add(omid)
                    
                    batch_count += 1
                
                print(f"  ✓ File complete. Rows: {batch_count}\n")
                
            except Exception as e:
                print(f"  ✗ Error processing file: {str(e)}")
                print(f"  → Skipping this file and continuing...\n")
                continue

print(f"✓ Step 3 complete. Found {len(venue_data)} matches with venue data")
print(f"  Found {len(omids_without_venue)} OMIDs without venue data\n")

# Save venue_data to external file
with open('venue_data_debug.json', 'w') as f:
    json.dump(venue_data, f, indent=2)
print("✓ Saved venue_data to venue_data_debug.json\n")

# Save OMIDs without venue data
with open('omids_without_venue.json', 'w') as f:
    json.dump(list(omids_without_venue), f, indent=2)
print(f"✓ Saved {len(omids_without_venue)} OMIDs without venue data to omids_without_venue.json\n")

#step 4: add venue data back into iris_oc_index dataframe
print("Step 4: Merging venue data with citations...")

iris_oc_index["citing_venue"] = iris_oc_index["citing"].map(venue_data)
iris_oc_index["cited_venue"] = iris_oc_index["cited"].map(venue_data)

# Save only rows where either citing_venue or cited_venue are not NaN
matched_rows = iris_oc_index[(iris_oc_index["citing_venue"].notna()) | (iris_oc_index["cited_venue"].notna())]
matched_rows = matched_rows.reindex(columns=['id', 'citing', 'citing_venue', 'cited', 'cited_venue', 'creation', 'is_citing_iris', 'is_cited_iris', 'flow'])
matched_rows.to_csv('iris_oc_venues_matched.csv', index=False)

print(f"✓ Saved {len(matched_rows)} rows with venue data to iris_oc_venues_matched.csv\n")

# Calculate and print elapsed time
end_time = time.time()
elapsed_time = end_time - start_time
minutes = int(elapsed_time // 60)
seconds = int(elapsed_time % 60)

print("✓✓✓ All steps complete! ✓✓✓")
print(f"\nTotal execution time: {minutes}m {seconds}s")