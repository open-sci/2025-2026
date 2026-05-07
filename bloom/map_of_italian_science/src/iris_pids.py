import os
import json
import time
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

# ==============================================================================
# ENVIRONMENT
# ==============================================================================

ROOT_DIR = Path(__file__).resolve().parent.parent

load_dotenv(ROOT_DIR / ".env")
DATA_PATH = os.environ.get("DATA_PATH")

if not DATA_PATH:
    raise RuntimeError("Missing DATA_PATH environment variable")


# ==============================================================================
# CONSTANTS AND CONFIGURATION
# ==============================================================================

# Paths and directories
DATA_DIR = Path(DATA_PATH)
IRIS_DIR = DATA_DIR / "iris"
OC_INDEX_PATH = DATA_DIR / "oc_index.sqlite3"
OUTPUT_DIR = ROOT_DIR / "output"

# File templates
INDEX_CSV_TEMPLATE = IRIS_DIR / "{university}" / "iris_in_oc_index" / "iris_in_oc_index.csv"
OUTPUT_PIDS_TEMPLATE = OUTPUT_DIR / "{university}" / "iris_pids.csv"
OUTPUT_MISSING_PIDS_TEMPLATE = OUTPUT_DIR / "{university}" / "iris_pids.missing.csv"
OUTPUT_METADATA_TEMPLATE = OUTPUT_DIR / "{university}" / "iris_pids.metadata.json"

# CSV writing configuration
WRITE_CSV_EVERY = 5000

# Universities with IRIS data available to process
IRIS_UNIVERSITIES = ("SNS", "UNIBO", "UNIMI", "UNIPD", "UNITO", "UPO")


# ==============================================================================
# DATABASE CONNECTION AND SETUP
# ==============================================================================

# Connect to the SQLite index database
OC_INDEX_DB = sqlite3.connect(OC_INDEX_PATH)

# Set row factory for dict-like access
OC_INDEX_DB.row_factory = sqlite3.Row

# Optimize SQLite performance for read-only access
OC_INDEX_DB.execute("PRAGMA query_only = ON")
OC_INDEX_DB.execute("PRAGMA temp_store = MEMORY")
OC_INDEX_DB.execute("PRAGMA cache_size = -500000")  # ~500 MB
OC_INDEX_DB.execute("PRAGMA mmap_size = 30000000000")  # ~30 GB


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
        "SELECT id, pub_date FROM meta WHERE omid = ?", (omid,)
    ).fetchone()

    if record is None:
        return None

    return extract_meta_values(dict(record))


def append_rows(path, rows):
    """Append rows to a CSV file, writing the header only if the file doesn't exist."""
    if not rows:
        return

    write_header = not path.exists()

    pd.DataFrame(rows).to_csv(
        path,
        mode="a",
        index=False,
        header=write_header,
    )


# ==============================================================================
# RUNTIME
# ==============================================================================

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(exist_ok=True)

# Iterate over each university
for university in IRIS_UNIVERSITIES:
    index_csv = Path(str(INDEX_CSV_TEMPLATE).format(university=university))
    output_csv = Path(str(OUTPUT_PIDS_TEMPLATE).format(university=university))
    missing_pids_csv = Path(str(OUTPUT_MISSING_PIDS_TEMPLATE).format(university=university))
    metadata_json = Path(str(OUTPUT_METADATA_TEMPLATE).format(university=university))

    # Create univerity-specific output directory if it doesn't exist
    output_csv.parent.mkdir(exist_ok=True)

    # Skip university if output CSV already exists
    if output_csv.exists():
        print(f"❗️ output CSV already exists for {university}, skipping: {output_csv.relative_to(OUTPUT_DIR)}")
        continue

    print(f"Processing university: {university}")
    print(f"Reading index from: {index_csv.relative_to(DATA_DIR)}")
    print(f"Writing output to: {output_csv.relative_to(OUTPUT_DIR)}")

    # Start monotonic timer
    started_at = time.monotonic()

    # Read the index CSV for the university
    index_df = pd.read_csv(index_csv)

    # Initialize counters
    processed_rows = []
    missing_rows = []
    rows_read = 0
    rows_processed = 0
    rows_missing_metadata = 0
    lookup_count = 0

    for index, row in index_df.iterrows():
        rows_read += 1
        direction = citation_direction(row)

        print(f"\n{index + 1}/{len(index_df)} Processing {row['id']} with direction: {direction}")

        oci = row["id"]
        citing_omid = row["citing"]
        cited_omid = row["cited"]

        print(f"  citing OMID: {citing_omid} -> cited OMID: {cited_omid}")

        citing_meta = lookup_oc_metadata(OC_INDEX_DB, citing_omid)
        cited_meta = lookup_oc_metadata(OC_INDEX_DB, cited_omid)
        lookup_count += 2

        if citing_meta is None or cited_meta is None:
            missing_side = []

            if citing_meta is None:
                missing_side.append("citing")
                print(f"        ⚠️ missing metadata for citing OMID {citing_omid}")

            if cited_meta is None:
                missing_side.append("cited")
                print(f"        ⚠️ missing metadata for cited OMID {cited_omid}")

            missing_rows.append(
                {
                    "oci": oci,
                    "direction": direction,
                    "missing_metadata": ";".join(missing_side),
                    "citing_omid": citing_omid,
                    "cited_omid": cited_omid,
                }
            )

            rows_missing_metadata += 1
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

        rows_processed += 1

        if len(processed_rows) % WRITE_CSV_EVERY == 0:
            append_rows(output_csv, processed_rows)
            processed_rows.clear()

    append_rows(output_csv, processed_rows)
    append_rows(missing_pids_csv, missing_rows)

    ended_at = datetime.now(timezone.utc)
    elapsed_seconds = round(time.monotonic() - started_at, 2)
    output_size_bytes = output_csv.stat().st_size if output_csv.exists() else 0
    missing_output_size_bytes = missing_pids_csv.stat().st_size if missing_pids_csv.exists() else 0

    metadata = {
        "university": university,
        "elapsed_seconds": elapsed_seconds,
        "ended_at": ended_at.isoformat(),
        "input_csv": str(index_csv),
        "output_csv": str(output_csv),
        "missing_pids_csv": str(missing_pids_csv),
        "rows_read": rows_read,
        "rows_processed": rows_processed,
        "rows_missing_metadata": rows_missing_metadata,
        "sqlite_lookup_count": lookup_count,
        "write_csv_every": WRITE_CSV_EVERY,
        "output_csv_size_bytes": output_size_bytes,
        "output_csv_size_mb": round(output_size_bytes / 1024 / 1024, 2),
        "missing_pids_csv_size_bytes": missing_output_size_bytes,
        "missing_pids_csv_size_mb": round(missing_output_size_bytes / 1024 / 1024, 2),
    }

    # Write metadata to JSON file
    with metadata_json.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print(
        f"\n🎉 final CSV written: {rows_processed:,} records -> "
        f"{output_csv.relative_to(OUTPUT_DIR)}\n"
    )

    print(
        f"⚠️ missing metadata CSV written: {rows_missing_metadata:,} records -> "
        f"{missing_pids_csv.relative_to(OUTPUT_DIR)}"
    )

    print(f"Elapsed time: {metadata['elapsed_seconds']} seconds")
    print(f"Metadata written to: {metadata_json.relative_to(OUTPUT_DIR)}")

# Close the SQLite connection
OC_INDEX_DB.close()
