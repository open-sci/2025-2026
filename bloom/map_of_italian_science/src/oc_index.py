import csv
import re
import os
import json
import time
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

# ==============================================================================
# ENVIRONMENT
# ==============================================================================

# Root directory of the project
ROOT_DIR = Path(__file__).resolve().parent.parent

# Load ENV variables
load_dotenv(ROOT_DIR / ".env")
DATA_PATH = os.environ.get("DATA_PATH")

if not DATA_PATH:
    raise RuntimeError("Missing DATA_PATH environment variable")


# ==============================================================================
# CONSTANTS AND CONFIGURATION
# ==============================================================================

# Define paths
DATA_DIR = Path(DATA_PATH)
CSV_DIR = DATA_DIR / "oc_csv"
DB_PATH = DATA_DIR / "oc_index.sqlite3"
METADATA_PATH = DATA_DIR / "metadata.json"

# Number of rows to insert before committing to SQLite
COMMIT_EVERY = 50_000

# Regular expression to extract OMID from the "id" field
OMID_RE = re.compile(r"\bomid:[^\s\]]+")

# SQL statement for inserting data
INSERT_SQL = """
INSERT OR REPLACE INTO meta (
  omid,
  id,
  venue,
  pub_date
) VALUES (?, ?, ?, ?)
"""


# ==============================================================================
# DATABASE CONNECTION AND SETUP
# ==============================================================================

# Connect to SQLite
OC_INDEX_DB = sqlite3.connect(DB_PATH)

# Set PRAGMA for performance
OC_INDEX_DB.execute("PRAGMA journal_mode = WAL")
OC_INDEX_DB.execute("PRAGMA synchronous = NORMAL")
OC_INDEX_DB.execute("PRAGMA temp_store = MEMORY")
OC_INDEX_DB.execute("PRAGMA cache_size = -200000")  # ~200 MB

# Create the "meta" table
OC_INDEX_DB.execute("""
CREATE TABLE IF NOT EXISTS meta (
  omid TEXT PRIMARY KEY,
  id TEXT NOT NULL,
  venue TEXT,
  pub_date TEXT
) WITHOUT ROWID
""")


# ==============================================================================
# RUNTIME
# ==============================================================================

# Start monotonic timer
started_at = time.monotonic()

# Gather all CSV files
csv_files = sorted(CSV_DIR.glob("*.csv"))
total_files = len(csv_files)

# Initialize counters
batch = []
total_rows = 0
total_committed = 0
skipped_without_omid = 0

print(f"Found {total_files:,} CSV files")
print(f"Writing SQLite index to {DB_PATH.relative_to(ROOT_DIR)}")

# Process each CSV file and insert rows into SQLite
for index, csv_file in enumerate(csv_files, start=1):
    files_left = total_files - index

    print(
        f"[{index:,}/{total_files:,}] Processing {csv_file.name} "
        f"({files_left:,} files left)"
    )

    rows_in_file = 0
    rows_added_from_file = 0

    with csv_file.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            rows_in_file += 1
            total_rows += 1

            # Extract OMID from the "id" field
            match = OMID_RE.search(row["id"])

            # If no OMID is found, skip this row and log it
            if not match:
                skipped_without_omid += 1
                continue

            omid = match.group(0)

            batch.append(
                (
                    omid,
                    row.get("id"),
                    row.get("venue"),
                    row.get("pub_date"),
                )
            )

            rows_added_from_file += 1

            # Commit in batches
            if len(batch) >= COMMIT_EVERY:
                print(
                    f"  Committing {len(batch):,} rows to SQLite "
                    f"(total rows seen: {total_rows:,})"
                )

                OC_INDEX_DB.executemany(INSERT_SQL, batch)
                OC_INDEX_DB.commit()

                total_committed += len(batch)
                batch.clear()

    print(
        f"  Done {csv_file.name}: "
        f"{rows_in_file:,} rows read, "
        f"{rows_added_from_file:,} rows added"
    )

if batch:
    print(f"Final commit: {len(batch):,} rows")

    OC_INDEX_DB.executemany(INSERT_SQL, batch)
    OC_INDEX_DB.commit()

    total_committed += len(batch)
    batch.clear()

# Close the connection to SQLite
OC_INDEX_DB.close()

# Compute elapsed time and database file size
ended_at = datetime.now(timezone.utc)
elapsed_seconds = round(time.monotonic() - started_at, 2)
db_size_bytes = DB_PATH.stat().st_size if DB_PATH.exists() else 0

# Aggregate metadata
metadata = {
    "elapsed_seconds": elapsed_seconds,
    "elapsed_human": str(datetime.fromtimestamp(elapsed_seconds, tz=timezone.utc).time()),
    "ended_at": ended_at.isoformat(),
    "files_processed": total_files,
    "rows_read": total_rows,
    "rows_committed": total_committed,
    "rows_skipped_without_omid": skipped_without_omid,
    "sqlite_file_size_bytes": db_size_bytes,
    "sqlite_file_size_mb": round(db_size_bytes / 1024 / 1024, 2),
    "sqlite_file_size_gb": round(db_size_bytes / 1024 / 1024 / 1024, 2),
}

with METADATA_PATH.open("w", encoding="utf-8") as file:
    json.dump(metadata, file, indent=2)

# Final summary
print("Done")
print(f"Files processed: {total_files:,}")
print(f"Rows read: {total_rows:,}")
print(f"Rows committed: {total_committed:,}")
print(f"Rows skipped without OMID: {skipped_without_omid:,}")
print(f"SQLite DB size: {metadata['sqlite_file_size_gb']} GB")
print(f"Elapsed time: {elapsed_seconds:,} seconds")
print(f"Summary written to: {METADATA_PATH.relative_to(ROOT_DIR)}")
