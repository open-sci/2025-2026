import json
import os
import time
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import requests

# Directories for input data and output results
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
OUTPUT_DIR = ROOT_DIR / "output"
CACHE_DIR = ROOT_DIR / "cache"

# Universities with IRIS data available
IRIS_UNIVERSITIES = sorted(path.name for path in DATA_DIR.iterdir() if path.is_dir())

# File templates
INDEX_CSV_TEMPLATE = DATA_DIR / "{university}" / "iris_in_oc_index" / "iris_in_oc_index.csv"
META_CSV_TEMPLATE = DATA_DIR / "{university}" / "iris_in_oc_meta" / "iris_in_oc_meta.csv"
OUTPUT_CSV_TEMPLATE = OUTPUT_DIR / "{university}" / "1a.csv"

# API endpoints
OC_META_API = "https://api.opencitations.net/meta/v1/metadata/"
OPENAIRE_API = "https://api.openaire.eu/graph/v2/researchProducts"
ROR_API = "https://api.ror.org/v2/organizations/"

# API request configuration
API_SLEEP_INTERVAL = 0.5
API_RETRIES = 2

# DataFrame schema for output CSV
DF_SCHEMA = {
    "oci": "string",
    "direction": "category",
    "citing_omid": "string",
    "citing_doi": "string",
    "citing_pub_date": "string",
    "cited_omid": "string",
    "cited_doi": "string",
    "cited_pub_date": "string",
}


# Create output and cache directories if they don't exist
OUTPUT_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

# API authentication
load_dotenv(ROOT_DIR / ".env")
OPENCITATIONS_AUTH_TOKEN = os.environ.get("OPENCITATIONS_AUTH_TOKEN")

if not OPENCITATIONS_AUTH_TOKEN:
    raise RuntimeError("Missing OPENCITATIONS_AUTH_TOKEN")


def safe_key(s):
    """Turn an identifier like 'omid:br/12345' into a filesystem-safe filename."""
    return s.replace("/", "_").replace(":", "_")


def cached_get(url, cache_key, headers=None, params=None):
    """GET a URL with a JSON cache on disk. Returns parsed JSON or None on failure."""
    cache_file = CACHE_DIR / f"{cache_key}.json"

    if cache_file.exists():
        return json.loads(cache_file.read_text())

    for _attempt in range(API_RETRIES + 1):
        time.sleep(API_SLEEP_INTERVAL)

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                cache_file.write_text(json.dumps(data))
                return data

            print(f"request failed: {url} status={response.status_code}")
        except Exception as e:
            print(f"request failed: {url} ({e})")

    return None


def citation_direction(df_row):
    """Determine citation direction for a row based on is_citing_iris and is_cited_iris flags."""
    citing = bool(df_row["is_citing_iris"])
    cited = bool(df_row["is_cited_iris"])

    if citing and cited:
        return "internal"
    if citing and not cited:
        return "outbound"
    if not citing and cited:
        return "inbound"

    raise ValueError(
        f"Invalid citation direction for row id={df_row.get('id')}: "
        "both is_citing_iris and is_cited_iris are False"
    )


def fetch_oc_metadata(omid):
    """Return {omid, doi, pub_date} for an OMID via the OpenCitations Meta API, or None."""
    headers = {"authorization": OPENCITATIONS_AUTH_TOKEN}
    data = cached_get(OC_META_API + omid, f"ocmeta_{safe_key(omid)}", headers=headers)

    if not data:
        return None

    entry = data[0] if isinstance(data, list) and data else data

    doi = None
    for tok in entry.get("id", "").split():
        if tok.startswith("doi:"):
            doi = strip_doi_prefix(tok)
            break

    return {"doi": doi, "pub_date": entry.get("pub_date")}


def strip_doi_prefix(string):
    """Remove 'doi:' prefix from a string if present."""
    return string[4:] if string.startswith("doi:") else string


def meta_by_omid(df):
    """Convert a metadata DataFrame into a dict mapping OMID to {doi, pub_date}."""
    cols = ["doi", "pub_date"]
    return (
        df.assign(doi=df["doi"].apply(strip_doi_prefix))
          .set_index("omid")[cols]
          .to_dict(orient="index")
    )


# Iterate over each university
for university in IRIS_UNIVERSITIES:
    index_csv = Path(str(INDEX_CSV_TEMPLATE).format(university=university))
    meta_csv = Path(str(META_CSV_TEMPLATE).format(university=university))
    output_csv = Path(str(OUTPUT_CSV_TEMPLATE).format(university=university))

    print(f"Processing university: {university}")
    print(f"Reading index from: {index_csv}")
    print(f"Reading metadata from: {meta_csv}")
    print(f"Writing output to: {output_csv}")

    index_df = pd.read_csv(index_csv)
    meta_df = pd.read_csv(meta_csv)
    meta_lookup = meta_by_omid(meta_df)

    rows = []
    for _, row in index_df.iterrows():
        direction = citation_direction(row)
        print(f"\nProcessing {row['id']} with direction: {direction}")

        oci = row["id"]
        citing_omid = row["citing"]
        cited_omid = row["cited"]
        citing_meta = None
        cited_meta = None

        if direction == "internal":
            print(f"   🔄 citing in IRIS ({citing_omid}), cited in IRIS: ({cited_omid})")
            citing_meta = meta_lookup.get(citing_omid)
            print(f"      lookup: {citing_meta}")
            cited_meta = meta_lookup.get(cited_omid)
            print(f"      lookup: {cited_meta}")
        elif direction == "outbound":
            print(f"   ↗️ citing in IRIS ({citing_omid}), cited is external: ({cited_omid})")
            citing_meta = meta_lookup.get(citing_omid)
            print(f"      lookup: {citing_meta}")
            cited_meta = fetch_oc_metadata(cited_omid)
            print(f"      fetch: {cited_meta}")
        elif direction == "inbound":
            print(f"   ↙️ citing is external ({citing_omid}), cited in IRIS: ({cited_omid})")
            citing_meta = fetch_oc_metadata(citing_omid)
            print(f"      fetch: {citing_meta}")
            cited_meta = meta_lookup.get(cited_omid)
            print(f"      lookup: {cited_meta}")

        rows.append(
            {
                "oci": row["id"],
                "direction": direction,
                "citing_omid": citing_omid,
                "citing_doi": citing_meta.get("doi"),
                "citing_pub_date": citing_meta.get("pub_date"),
                "cited_omid": cited_omid,
                "cited_doi": cited_meta.get("doi"),
                "cited_pub_date": cited_meta.get("pub_date"),
            }
        )

    final_df = pd.DataFrame(rows, columns=DF_SCHEMA.keys()).astype(DF_SCHEMA)

    # print(final_df.to_string())
    output_csv.parent.mkdir(exist_ok=True)
    final_df.to_csv(output_csv, index=False)
