from requests import get
import pandas as pd
###First test with full dataset - orginal csv is way to big to make only two API requests 
# - would maybe need to loop through batches and merge into a single output 
# - or see how to handle API requests for large data

#This script takes in the path for the iris_in_oc_index.csv to expand it with a citation flow column. It then runs a call to the OC API with a list of the citing omid and cited omid in order to return the corresponding venue data. The new expanded csv is saved as a new file, iris_oc_venue.csv

#step 1: import iris_oc_index as dataframe
print("Step 1: Importing iris_oc_index...")
iris_oc_index = pd.read_csv("bloom/disciplinary_flow/SNS/IRIS_OC/iris_in_oc_index/iris_in_oc_index.csv", nrows=180)
print(f"✓ Step 1 complete. Loaded {len(iris_oc_index)} rows\n")

#step 1.1: for clarity sake add "flow" column to define: outgoing, incoming and internal citations
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

#step 2: create two strings (sep with: __): one for citing omids and one for cited omids
print("Step 2: Creating lists of citing and cited OMIDs...")
# citing_omids = "__".join(iris_oc_index["citing"].unique().tolist())
# cited_omids = "__".join(iris_oc_index["cited"].unique().tolist())
citing_omids = iris_oc_index["citing"].unique().tolist()
cited_omids = iris_oc_index["cited"].unique().tolist()
print(f"✓ Step 2 complete. Citing OMIDs: {len(iris_oc_index["citing"].unique().tolist())}, Cited OMIDs: {len(iris_oc_index["cited"].unique().tolist())}\n")

#step 3: make API call to OC with the omids
#step 3.1: make API call for citing omids
print("3.1: make API call for citing omids...\n")
citing_alldata = []
for omid in citing_omids:
    CITING_API_CALL = f"https://api.opencitations.net/meta/v1/metadata/{omid}?require=venue&format=csv"
    HTTP_HEADERS = {"authorization": "02637892-4b59-4f19-acae-72f162afde5e-1776847862"}

    response = get(CITING_API_CALL, headers=HTTP_HEADERS)
    citing_alldata.append(response.text)

# with open('bloom/disciplinary_flow/SNS/sns_citing_meta.csv', 'w') as f:
#     f.write(cited_alldata)
with open('bloom/disciplinary_flow/SNS/sns_citing_meta.csv', 'w') as f:
    for idx, batch_data in enumerate(citing_alldata):
        if idx > 0:
            lines = batch_data.split('\n')
            f.write('\n'.join(lines[1:]))
        else:
            f.write(batch_data)

###check what's happening with this loop to make the csv

#step 3.2: make API call for cited omids
# print("3.1: make API call for cited omids...\n")
# CITING_API_CALL = f"https://api.opencitations.net/meta/v1/metadata/{cited_omids}?require=venue&format=csv"
# HTTP_HEADERS = {"authorization": "02637892-4b59-4f19-acae-72f162afde5e-1776847862"}

# cited_meta = get(CITING_API_CALL, headers=HTTP_HEADERS)
# with open('bloom/disciplinary_flow/SNS/sns_cited_meta.csv', 'w') as f:
#     f.write(citing_meta.text)

#step 4: add venue data back into iris_oc_index dataframe
# print("Step 4: Adding venue data back to iris_oc_index...")
# citing_meta_venue = pd.read_csv('bloom/disciplinary_flow/SNS/sns_citing_meta.csv', usecols=["id","venue"])
# cited_meta_venue = pd.read_csv('bloom/disciplinary_flow/SNS/sns_cited_meta.csv', usecols=["id","venue"])
# print("✓ Loaded venue data\n")

#step 4.1 extract omid in id column which will be used to join these df to the main iris_oc_index df + rename columns
# print("Step 4.1: Extracting OMIDs and renaming columns...")
# def extract_omid(row):
#     omid = row["id"].split(" ")[-1]
#     return omid

# citing_meta_venue["id"] = citing_meta_venue.apply(extract_omid, axis=1)
# cited_meta_venue["id"] = cited_meta_venue.apply(extract_omid, axis=1)

# citing_meta_venue = citing_meta_venue.rename(columns={"id":"citing_id",'venue': 'citing_venue'})
# cited_meta_venue = cited_meta_venue.rename(columns={"id":"cited_id",'venue': 'cited_venue'})
# print("✓ Step 4.1 complete\n")

#step 4.2 use merge to combine all three dataframes
# print("Step 4.2: Merging dataframes...")
# iris_oc_venues = iris_oc_index.merge(citing_meta_venue, left_on='citing', right_on="citing_id", how='left')
# iris_oc_venues = iris_oc_venues.merge(cited_meta_venue, left_on='cited', right_on='cited_id', how='left')
# iris_oc_venues = iris_oc_venues.drop(["citing_id", "cited_id"], axis=1)
# iris_oc_venues = iris_oc_venues.reindex(columns=['id', 'citing', 'citing_venue', 'cited', 'cited_venue', 'creation', 'is_citing_iris', 'is_cited_iris', 'flow'])

# print("Step 4.3: Saving to CSV...")
# iris_oc_venues.to_csv('bloom/disciplinary_flow/SNS/sns_citations_venues.csv', index=False)
# print("✓✓✓ All steps complete! Output saved to sns_citations_venues.csv ✓✓✓")