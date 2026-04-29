from requests import get
import pandas as pd

#This script takes in the path for the iris_in_oc_index.csv to expand it with a citation flow column. It then runs a call to the OC API with a list of the citing omid and cited omid in order to return the corresponding venue data. The new expanded csv is saved as a new file, iris_oc_venue.csv

#step 1: import iris_oc_index as dataframe
iris_oc_index = pd.read_csv("IRIS_OC/SNS/iris_in_oc_index/iris_in_oc_index.csv", nrows=20)

#step 1.1: for clarity sake add "flow" column to define: outgoing, incoming and internal citations
def classify_flow(row):
    if row["is_citing_iris"] and row["is_cited_iris"]:
        return "Internal"
    elif row["is_citing_iris"] and not row["is_cited_iris"]:
        return "Outgoing"
    elif not row["is_citing_iris"] and row["is_cited_iris"]:
        return "Incoming"

iris_oc_index["flow"] = iris_oc_index.apply(classify_flow, axis=1)


#step 2: create two strings (sep with: __): one for citing omids and one for cited omids
citing_omids = "__".join(iris_oc_index["citing"].unique().tolist())
cited_omids = "__".join(iris_oc_index["cited"].unique().tolist())
# print(len(iris_oc_index["citing"].unique().tolist()))
# print(len(iris_oc_index["cited"].unique().tolist()))

#step 3: make API call to OC with the omids
#step 3.1: make API call for citing omids
CITING_API_CALL = f"https://api.opencitations.net/meta/v1/metadata/{citing_omids}?require=venue&format=csv"
HTTP_HEADERS = {"authorization": "02637892-4b59-4f19-acae-72f162afde5e-1776847862"}

citing_meta = get(CITING_API_CALL, headers=HTTP_HEADERS)
with open('citing_meta.csv', 'w') as f:
    f.write(citing_meta.text)

#step 3.2: make API call for cited omids
CITED_API_CALL = f"https://api.opencitations.net/meta/v1/metadata/{cited_omids}?require=venue&format=csv"
HTTP_HEADERS = {"authorization": "02637892-4b59-4f19-acae-72f162afde5e-1776847862"}

citing_meta = get(CITING_API_CALL, headers=HTTP_HEADERS)
with open('cited_meta.csv', 'w') as f:
    f.write(citing_meta.text)

#step 4: add venue data back into iris_oc_index dataframe
citing_meta_venue = pd.read_csv("citing_meta.csv", usecols=["id","venue"])
cited_meta_venue = pd.read_csv("cited_meta.csv", usecols=["id","venue"])

#step 4.1 extract omid in id column which will be used to join these df to the main iris_oc_index df + rename columns
def extract_omid(row):
    omid = row["id"].split(" ")[-1]
    return omid

citing_meta_venue["id"] = citing_meta_venue.apply(extract_omid, axis=1)
cited_meta_venue["id"] = cited_meta_venue.apply(extract_omid, axis=1)

citing_meta_venue = citing_meta_venue.rename(columns={"id":"citing_id",'venue': 'citing_venue'})
cited_meta_venue = cited_meta_venue.rename(columns={"id":"cited_id",'venue': 'cited_venue'})

#step 4.2 use merge to combine all three dataframes
iris_oc_venues = iris_oc_index.merge(citing_meta_venue, left_on='citing', right_on="citing_id", how='left')
iris_oc_venues = iris_oc_venues.merge(cited_meta_venue, left_on='cited', right_on='cited_id', how='left')
iris_oc_venues = iris_oc_venues.drop(["citing_id", "cited_id"], axis=1)
iris_oc_venues = iris_oc_venues.reindex(columns=['id', 'citing', 'citing_venue', 'cited', 'cited_venue', 'creation', 'is_citing_iris', 'is_cited_iris', 'flow'])

iris_oc_venues.to_csv('iris_oc_venues.csv', index=False)
