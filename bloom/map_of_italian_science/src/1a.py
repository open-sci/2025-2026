import json
import os
import time
import sqlite3
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import requests

# ==============================================================================
# CONSTANTS AND CONFIGURATION
# ==============================================================================

# Directories for input/output
ROOT_DIR = Path(__file__).resolve().parent.parent

# Load ENV variables
load_dotenv(ROOT_DIR / ".env")
STORAGE_PATH = os.environ.get("STORAGE_PATH")
OPENCITATIONS_AUTH_TOKEN = os.environ.get("OPENCITATIONS_AUTH_TOKEN")

if not OPENCITATIONS_AUTH_TOKEN:
    raise RuntimeError("Missing OPENCITATIONS_AUTH_TOKEN")

if not STORAGE_PATH:
    raise RuntimeError("Missing STORAGE_PATH environment variable")

DATA_DIR = ROOT_DIR / "data"
OUTPUT_DIR = ROOT_DIR / "output"
CACHE_DIR = ROOT_DIR / "cache"

STORAGE_DIR = Path(STORAGE_PATH)
OC_INDEX_PATH = STORAGE_DIR / "oc_index.sqlite3"

# Universities with IRIS data available
IRIS_UNIVERSITIES = sorted(path.name for path in DATA_DIR.iterdir() if path.is_dir())

# File templates
INDEX_CSV_TEMPLATE = DATA_DIR / "{university}" / "iris_in_oc_index" / "iris_in_oc_index.csv"
META_CSV_TEMPLATE = DATA_DIR / "{university}" / "iris_in_oc_meta" / "iris_in_oc_meta.csv"
OUTPUT_CSV_TEMPLATE = OUTPUT_DIR / "{university}" / "1a.csv"

# API endpoints and configuration
API_OC_META_ENDPOINT = "https://api.opencitations.net/meta/v1/metadata/"
API_SLEEP_INTERVAL = 0.4
API_RETRIES = 3


# ==============================================================================
# METHODS
# ==============================================================================

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


def safe_key(s):
    """Turn an identifier like 'omid:br/12345' into a filesystem-safe filename."""
    return s.replace("/", "_").replace(":", "_")


def cached_get(url, cache_key, headers=None, params=None):
    """GET a URL with a JSON cache on disk. Returns parsed JSON or None on failure."""
    cache_file = CACHE_DIR / f"{cache_key}.json"

    if cache_file.exists():
        print("         cache hit:", url)
        return json.loads(cache_file.read_text())

    for attempt in range(API_RETRIES + 1):
        time.sleep(API_SLEEP_INTERVAL * (2 ** attempt))  # Exponential backoff

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                cache_file.write_text(json.dumps(data))
                print("         request success:", url)
                return data

            print(f"request failed: {url} status={response.status_code}")
        except Exception as e:
            print(f"request failed: {url} ({e})")

    return None


def fetch_oc_metadata(omid):
    """Return {doi, pmid, isbn, pub_date} for an OMID via the OpenCitations Meta API, or None."""
    headers = {"authorization": OPENCITATIONS_AUTH_TOKEN}
    data = cached_get(API_OC_META_ENDPOINT + omid, f"ocmeta_{safe_key(omid)}", headers=headers)

    if not data:
        return None

    entry = data[0] if isinstance(data, list) and data else data

    ids = {
        tok.split(":", 1)[0]: tok
        for tok in entry.get("id", "").split()
        if tok.startswith(("doi:", "pmid:", "isbn:"))
    }

    return {
        "doi": ids.get("doi"),
        "pmid": ids.get("pmid"),
        "isbn": ids.get("isbn"),
        "pub_date": entry.get("pub_date"),
    }

def meta_lookup(index_db, omid):
    print(f"         looking up in SQLite index: {omid}")
    record = index_db.execute(
        "SELECT * FROM meta WHERE omid = ?",
        (omid,)
    ).fetchone()

    if record is None:
        return None

    return dict(record)

# ==============================================================================
# RUNTIME
# ==============================================================================

# Connect to the SQLite index database
OC_INDEX_DB = sqlite3.connect(OC_INDEX_PATH)
OC_INDEX_DB.row_factory = sqlite3.Row

# Create output and cache directories if they don't exist
OUTPUT_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

# Iterate over each university
for university in IRIS_UNIVERSITIES:
    index_csv = Path(str(INDEX_CSV_TEMPLATE).format(university=university))
    meta_csv = Path(str(META_CSV_TEMPLATE).format(university=university))
    output_csv = Path(str(OUTPUT_CSV_TEMPLATE).format(university=university))

    print(f"Processing university: {university}")
    print(f"Reading index from: {index_csv.relative_to(ROOT_DIR)}")
    print(f"Reading metadata from: {meta_csv.relative_to(ROOT_DIR)}")
    print(f"Writing output to: {output_csv.relative_to(ROOT_DIR)}")

    index_df = pd.read_csv(index_csv)
    meta_df = pd.read_csv(meta_csv)

    rows = []

    write_every = 100
    output_csv.parent.mkdir(exist_ok=True)

    for index, row in index_df.iterrows():
        direction = citation_direction(row)

        print(f"\n{index + 1}/{len(index_df)} Processing {row['id']} with direction: {direction}")

        oci = row["id"]
        citing_omid = row["citing"]
        cited_omid = row["cited"]
        citing_meta = meta_lookup(OC_INDEX_DB, citing_omid)
        cited_meta = meta_lookup(OC_INDEX_DB, cited_omid)

        if citing_meta is None:
            print(f"      ⚠️ skipping: no metadata found for citing OMID {citing_omid}")
            continue

        if cited_meta is None:
            print(f"      ⚠️ skipping: no metadata found for cited OMID {cited_omid}")
            continue

        rows.append(
            {
                "oci": row["id"],
                "direction": direction,
                "citing_omid": citing_omid,
                "citing_doi": citing_meta.get("doi"),
                "citing_pmid": citing_meta.get("pmid"),
                "citing_isbn": citing_meta.get("isbn"),
                "citing_pub_date": citing_meta.get("pub_date"),
                "cited_omid": cited_omid,
                "cited_doi": cited_meta.get("doi"),
                "cited_pmid": cited_meta.get("pmid"),
                "cited_isbn": cited_meta.get("isbn"),
                "cited_pub_date": cited_meta.get("pub_date"),
            }
        )

        if len(rows) % write_every == 0:
            pd.DataFrame(rows).to_csv(output_csv, index=False)
            print(f"\n💾 checkpoint written: {len(rows)} records -> {output_csv.relative_to(ROOT_DIR)}")

    final_df = pd.DataFrame(rows)
    final_df.to_csv(output_csv, index=False)
    print(f"   ✅ final CSV written: {len(rows)} records -> {output_csv.relative_to(ROOT_DIR)}\n\n")

# Close the SQLite connection
OC_INDEX_DB.close()
