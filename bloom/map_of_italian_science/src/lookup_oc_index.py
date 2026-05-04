import os
import sys
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
DB_PATH = STORAGE_DIR / "oc_index.sqlite3"

omid = sys.argv[1]

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

row = conn.execute(
    "SELECT * FROM meta WHERE omid = ?",
    (omid,)
).fetchone()

if row is None:
    print("Not found")
else:
    print(dict(row))

conn.close()
