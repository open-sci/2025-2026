import os
import sqlite3
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

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


def extract_meta_values(record):
    ids = {
        tok.split(":", 1)[0]: tok
        for tok in record.get("id", "").split()
        if tok.startswith(("doi:", "pmid:", "isbn:"))
    }

    return {
        "doi": ids.get("doi"),
        "pmid": ids.get("pmid"),
        "isbn": ids.get("isbn"),
        "pub_date": record.get("pub_date"),
    }


def lookup_oc_metadata(index_db, omid):
    print(f"    looking up in SQLite index: {omid}")

    record = index_db.execute(
        "SELECT * FROM meta WHERE omid = ?",
        (omid,)
    ).fetchone()

    if record is None:
        return None

    return extract_meta_values(dict(record))


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

    write_every = 5000
    output_csv.parent.mkdir(exist_ok=True)

    rows = []

    for index, row in index_df.iterrows():
        direction = citation_direction(row)

        print(f"\n{index + 1}/{len(index_df)} Processing {row['id']} with direction: {direction}")

        oci = row["id"]
        citing_omid = row["citing"]
        cited_omid = row["cited"]

        print(f"  Citing OMID: {citing_omid} -> Cited OMID: {cited_omid}")

        citing_meta = lookup_oc_metadata(OC_INDEX_DB, citing_omid)
        cited_meta = lookup_oc_metadata(OC_INDEX_DB, cited_omid)

        if citing_meta is None:
            print(f"        ⚠️ skipping: no metadata found for citing OMID {citing_omid}")
            continue

        if cited_meta is None:
            print(f"        ⚠️ skipping: no metadata found for cited OMID {cited_omid}")
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
    print(f"\n✅ final CSV written: {len(rows)} records -> {output_csv.relative_to(ROOT_DIR)}\n\n")

# Close the SQLite connection
OC_INDEX_DB.close()
