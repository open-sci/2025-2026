import csv
import re
import os
import sqlite3
from pathlib import Path
from dotenv import load_dotenv

# Root directory of the project
ROOT_DIR = Path(__file__).resolve().parent.parent

# Load ENV variables
load_dotenv(ROOT_DIR / ".env")
STORAGE_PATH = os.environ.get("STORAGE_PATH")

if not STORAGE_PATH:
    raise RuntimeError("Missing STORAGE_PATH environment variable")

# Define paths
STORAGE_DIR = Path(STORAGE_PATH)
CSV_DIR = STORAGE_DIR / "csv"
DB_PATH = STORAGE_DIR / "oc_index.sqlite3"

# Regular expression to extract OMID from the "id" field
OMID_RE = re.compile(r"\bomid:[^\s\]]+")

# Connect to SQLite
conn = sqlite3.connect(DB_PATH)

# Set PRAGMA for performance
conn.execute("PRAGMA journal_mode = WAL")
conn.execute("PRAGMA synchronous = NORMAL")
conn.execute("PRAGMA temp_store = MEMORY")
conn.execute("PRAGMA cache_size = -200000")  # ~200 MB

# Create the "meta" table
conn.execute("""
CREATE TABLE IF NOT EXISTS meta (
  omid TEXT PRIMARY KEY,
  id TEXT NOT NULL,
  title TEXT,
  author TEXT,
  issue TEXT,
  volume TEXT,
  venue TEXT,
  page TEXT,
  pub_date TEXT,
  type TEXT,
  publisher TEXT,
  editor TEXT
) WITHOUT ROWID
""")

insert_sql = """
INSERT OR REPLACE INTO meta (
  omid,
  id,
  title,
  author,
  issue,
  volume,
  venue,
  page,
  pub_date,
  type,
  publisher,
  editor
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Gather all CSV files
csv_files = sorted(CSV_DIR.glob("*.csv"))
total_files = len(csv_files)

print(f"Found {total_files:,} CSV files")
print(f"Writing SQLite index to {DB_PATH}")


batch = []
total_rows = 0
total_committed = 0
skipped_without_omid = 0

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
                    row.get("title"),
                    row.get("author"),
                    row.get("issue"),
                    row.get("volume"),
                    row.get("venue"),
                    row.get("page"),
                    row.get("pub_date"),
                    row.get("type"),
                    row.get("publisher"),
                    row.get("editor"),
                )
            )

            rows_added_from_file += 1

            # Commit in batches of 50,000 rows
            if len(batch) >= 50_000:
                print(
                    f"  Committing {len(batch):,} rows to SQLite "
                    f"(total rows seen: {total_rows:,})"
                )

                conn.executemany(insert_sql, batch)
                conn.commit()

                total_committed += len(batch)
                batch.clear()

    print(
        f"  Done {csv_file.name}: "
        f"{rows_in_file:,} rows read, "
        f"{rows_added_from_file:,} rows added"
    )

if batch:
    print(f"Final commit: {len(batch):,} rows")

    conn.executemany(insert_sql, batch)
    conn.commit()

    total_committed += len(batch)
    batch.clear()

# print("Running ANALYZE")
# conn.execute("ANALYZE")

# Close the connection to SQLite
conn.close()

# Final summary
print("Done")
print(f"Files processed: {total_files:,}")
print(f"Rows read: {total_rows:,}")
print(f"Rows committed: {total_committed:,}")
print(f"Rows skipped without OMID: {skipped_without_omid:,}")
