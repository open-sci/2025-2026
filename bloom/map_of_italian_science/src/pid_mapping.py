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

if not STORAGE_PATH:
    raise RuntimeError("Missing STORAGE_PATH environment variable")

DATA_DIR = ROOT_DIR / "data"
OUTPUT_DIR = ROOT_DIR / "output"

STORAGE_DIR = Path(STORAGE_PATH)
OC_INDEX_PATH = STORAGE_DIR / "oc_index.sqlite3"

WRITE_CSV_EVERY = 5000

# File templates
INDEX_CSV_TEMPLATE = DATA_DIR / "{university}" / "iris_in_oc_index" / "iris_in_oc_index.csv"
OUTPUT_CSV_TEMPLATE = OUTPUT_DIR / "{university}" / "pid_mapping.csv"
MISSING_META_CSV_TEMPLATE = OUTPUT_DIR / "{university}" / "pid_mapping_missing_metad.csv"
OUTPUT_LOG_TEMPLATE = OUTPUT_DIR / "{university}" / "pid_mapping.log"

# Universities with IRIS data available to process
IRIS_UNIVERSITIES = ("SNS", "UNIBO", "UNIMI", "UNIPD", "UNITO", "UPO")


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
    """Extract DOI, PMID, ISBN, and publication date from the metadata record."""
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
    """Lookup metadata for a given OMID in the SQLite index database."""
    record = index_db.execute(
        "SELECT * FROM meta WHERE omid = ?",
        (omid,)
    ).fetchone()

    if record is None:
        return None

    return extract_meta_values(dict(record))


def log(message="", file=None):
    """Log a message to the console and optionally to a file."""
    print(message)

    if file is not None:
        file.write(f"{message}\n")
        file.flush()


# ==============================================================================
# RUNTIME
# ==============================================================================

# Connect to the SQLite index database and set row factory for dict-like access
OC_INDEX_DB = sqlite3.connect(OC_INDEX_PATH)
OC_INDEX_DB.row_factory = sqlite3.Row

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(exist_ok=True)

# Iterate over each university
for university in IRIS_UNIVERSITIES:
    index_csv = Path(str(INDEX_CSV_TEMPLATE).format(university=university))
    output_csv = Path(str(OUTPUT_CSV_TEMPLATE).format(university=university))
    output_log = Path(str(OUTPUT_LOG_TEMPLATE).format(university=university))
    missing_meta_csv = Path(str(MISSING_META_CSV_TEMPLATE).format(university=university))

    output_csv.parent.mkdir(exist_ok=True)

    if output_csv.exists():
        print(f"❗️ output CSV already exists for {university}, skipping: {output_csv.relative_to(ROOT_DIR)}")
        continue

    with output_log.open("w", encoding="utf-8") as log_file:
        log(f"Processing university: {university}", log_file)
        log(f"Reading index from: {index_csv.relative_to(ROOT_DIR)}", log_file)
        log(f"Writing output to: {output_csv.relative_to(ROOT_DIR)}", log_file)
        log(f"Writing log to: {output_log.relative_to(ROOT_DIR)}", log_file)

        index_df = pd.read_csv(index_csv)

        processed_rows = []
        missing_rows = []

        for index, row in index_df.iterrows():
            direction = citation_direction(row)

            log(f"\n{index + 1}/{len(index_df)} Processing {row['id']} with direction: {direction}", log_file)

            oci = row["id"]
            citing_omid = row["citing"]
            cited_omid = row["cited"]

            log(f"  citing OMID: {citing_omid} -> cited OMID: {cited_omid}", log_file)

            citing_meta = lookup_oc_metadata(OC_INDEX_DB, citing_omid)
            cited_meta = lookup_oc_metadata(OC_INDEX_DB, cited_omid)

            if citing_meta is None or cited_meta is None:
                missing_side = []

                if citing_meta is None:
                    missing_side.append("citing")
                    log(f"        ⚠️ missing metadata for citing OMID {citing_omid}", log_file)

                if cited_meta is None:
                    missing_side.append("cited")
                    log(f"        ⚠️ missing metadata for cited OMID {cited_omid}", log_file)

                missing_rows.append(
                    {
                        "oci": oci,
                        "direction": direction,
                        "missing_metadata": ";".join(missing_side),
                        "citing_omid": citing_omid,
                        "cited_omid": cited_omid,
                    }
                )

                continue

            processed_rows.append(
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

            if len(processed_rows) % WRITE_CSV_EVERY == 0:
                pd.DataFrame(processed_rows).to_csv(output_csv, index=False)
                log(f"\n💾 checkpoint written: {len(processed_rows)} records -> "
                    f"{output_csv.relative_to(ROOT_DIR)}",
                    log_file
                )

        final_df = pd.DataFrame(processed_rows)
        final_df.to_csv(output_csv, index=False)
        log(f"\n🎉 final CSV written: {len(processed_rows)} records -> "
            f"{output_csv.relative_to(ROOT_DIR)}\n",
            log_file
        )

        missing_df = pd.DataFrame(missing_rows)
        missing_df.to_csv(missing_meta_csv, index=False)

        log(
            f"⚠️ missing metadata CSV written: {len(missing_rows)} records -> "
            f"{missing_meta_csv.relative_to(ROOT_DIR)}",
            log_file
        )

# Close the SQLite connection
OC_INDEX_DB.close()
