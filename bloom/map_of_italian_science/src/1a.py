import json
import os
import time
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import requests

# ==============================================================================
# CONSTANTS AND CONFIGURATION
# ==============================================================================

# Directories for input/output
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
API_SLEEP_INTERVAL = 1
API_RETRIES = 2

# Metadata fields to consider for scoring completeness
META_COLS = ["doi", "pmid", "isbn", "pub_date"]


# ==============================================================================
# METHODS
# ==============================================================================

def is_present(value):
    """Treat NaN, None, and empty strings as missing."""
    if pd.isna(value):
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    return True


def entry_info_score(entry):
    """Count how many useful metadata fields are present."""
    return sum(is_present(entry[col]) for col in META_COLS)


def best_meta_for_omid(df, omid):
    """Return the metadata row for one OMID with the most information."""
    matches = df[df["omid"] == omid]

    if matches.empty:
        return None

    scored = matches.copy()
    scored["_info_score"] = scored.apply(entry_info_score, axis=1)

    best_row = scored.sort_values("_info_score", ascending=False).iloc[0]

    return best_row[META_COLS].to_dict()


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

    for _attempt in range(API_RETRIES + 1):
        time.sleep(API_SLEEP_INTERVAL)

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
    data = cached_get(OC_META_API + omid, f"ocmeta_{safe_key(omid)}", headers=headers)

    if not data:
        return None

    entry = data[0] if isinstance(data, list) and data else data

    doi, pmid, isbn = None, None, None

    for tok in entry.get("id", "").split():
        if tok.startswith("doi:"):
            doi = tok[4:]
        elif tok.startswith("pmid:"):
            pmid = tok[5:]
        elif tok.startswith("isbn:"):
            isbn = tok[5:]

    return {"doi": doi, "pmid": pmid, "isbn": isbn, "pub_date": entry.get("pub_date")}


# ==============================================================================
# RUNTIME
# ==============================================================================

# Load ENV variables
load_dotenv(ROOT_DIR / ".env")
OPENCITATIONS_AUTH_TOKEN = os.environ.get("OPENCITATIONS_AUTH_TOKEN")

if not OPENCITATIONS_AUTH_TOKEN:
    raise RuntimeError("Missing OPENCITATIONS_AUTH_TOKEN")

# Create output and cache directories if they don't exist
OUTPUT_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

# Iterate over each university
for university in IRIS_UNIVERSITIES[0:1]:
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
        citing_meta = None
        cited_meta = None

        if direction == "internal":
            print(f"   🔄 citing in IRIS ({citing_omid}), cited in IRIS: ({cited_omid})")
            citing_meta = best_meta_for_omid(meta_df, citing_omid)
            print(f"      lookup: {citing_meta}")
            cited_meta = best_meta_for_omid(meta_df, cited_omid)
            print(f"      lookup: {cited_meta}")
        elif direction == "outbound":
            print(f"   ↗️ citing in IRIS ({citing_omid}), cited is external: ({cited_omid})")
            citing_meta = best_meta_for_omid(meta_df, citing_omid)
            print(f"      lookup: {citing_meta}")
            cited_meta = fetch_oc_metadata(cited_omid)
            print(f"      fetch: {cited_meta}")
        elif direction == "inbound":
            print(f"   ↙️ citing is external ({citing_omid}), cited in IRIS: ({cited_omid})")
            citing_meta = fetch_oc_metadata(citing_omid)
            print(f"      fetch: {citing_meta}")
            cited_meta = best_meta_for_omid(meta_df, cited_omid)
            print(f"      lookup: {cited_meta}")

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
