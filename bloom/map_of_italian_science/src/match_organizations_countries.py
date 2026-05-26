import json
import gzip
import sys
import time
import tarfile
from datetime import datetime, timezone
from pathlib import Path

# ==============================================================================
# CONSTANTS AND CONFIGURATION
# ==============================================================================

# Paths and directories
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"

DUMPS_DIR = DATA_DIR / "dumps"
OPENAIRE_TAR = DUMPS_DIR / "openaire" / "organization.tar"
ROR_JSON = DUMPS_DIR / "ror" / "v2.7-2026-05-12-ror-data.json"

OUTPUT_DIR = DATA_DIR / "openaire_ror_countries"
OUTPUT_JSON = OUTPUT_DIR / "openaire_ror_countries.json"
OUTPUT_METADATA = OUTPUT_DIR / "openaire_ror_countries.metadata.json"

# Progress logging
LOG_EVERY = 50_000


# ==============================================================================
# METHODS
# ==============================================================================

def ror_display_name(names):
    """Return the ror_display name from a ROR names list, or None."""
    for name in names:
        if "ror_display" in (name.get("types") or []):
            return name.get("value")
    return None


def build_ror_country_index(ror_path):
    """Read ror.json and return a dict mapping each ROR id to its country and display name."""
    with open(ror_path, "r", encoding="utf-8") as path:
        ror_data = json.load(path)

    index = {}
    for record in ror_data:
        record_ror_id = record.get("id", "")
        locations = record.get("locations", [])
        if locations:
            geo = locations[0].get("geonames_details", {})
            index[record_ror_id] = {
                "country_name": geo.get("country_name", ""),
                "country_code": geo.get("country_code", ""),
                "display_name": ror_display_name(record.get("names", [])),
            }

    return index


def iter_openaire_orgs(tar_path):
    """Yield every JSON object from a tar archive of json.gz files.
    Each .gz member contains one JSON-Lines document (one record per line).
    """
    with tarfile.open(tar_path, "r") as tar:
        for member in tar:
            if not member.name.endswith(".gz"):
                continue

            gz_file = tar.extractfile(member)
            if gz_file is None:
                continue

            with gzip.open(gz_file, "rt", encoding="utf-8") as lines:
                for line in lines:
                    line = line.strip()
                    if line:
                        yield json.loads(line)


def extract_ror_id(pids):
    """Return the ROR id from an OpenAIRE pids list, or None."""
    for pid in pids:
        if pid.get("scheme") == "ROR":
            return pid["value"]
    return None


def extract_openaire_country(org_record):
    """Return (country_code, country_name) from the OpenAIRE country key, or (None, None)."""
    country = org_record.get("country")
    if country is None:
        return None, None

    code = country.get("code") or None
    label = country.get("label") or None

    if code is None and label is None:
        return None, None

    return code, label


# ==============================================================================
# RUNTIME
# ==============================================================================

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Skip if output JSON already exists
if OUTPUT_JSON.exists():
    print(f"! output JSON already exists, skipping: {OUTPUT_JSON.relative_to(DATA_DIR)}")
    sys.exit(0)

print("Building ROR country index…")
ror_index = build_ror_country_index(ROR_JSON)
print(f"  {len(ror_index):,} ROR entries loaded from {ROR_JSON.relative_to(DATA_DIR)}")

print(f"Scanning OpenAIRE organizations from {OPENAIRE_TAR.relative_to(DATA_DIR)}…")

# Start monotonic timer
started_at = time.monotonic()

# Initialize counters
result = {}
rows_read = 0
rows_matched_ror = 0
rows_ror_not_in_dump = 0
rows_no_ror_with_country = 0
rows_no_ror_no_country = 0

for org in iter_openaire_orgs(OPENAIRE_TAR):
    rows_read += 1

    ror_id = extract_ror_id(org.get("pids") or [])

    if ror_id is not None:
        # Organization has a ROR id: try to resolve country from ROR dump
        ror_country = ror_index.get(ror_id)

        if ror_country is not None:
            result[org["id"]] = {
                "legal_name": ror_country["display_name"] or org.get("legalName", ""),
                "country_name": ror_country["country_name"],
                "country_code": ror_country["country_code"],
                "country_source": "ror",
                "ror": ror_id,
            }
            rows_matched_ror += 1
        else:
            rows_ror_not_in_dump += 1
    else:
        # No ROR id: fall back to OpenAIRE country field
        oa_code, oa_label = extract_openaire_country(org)

        if oa_code is not None or oa_label is not None:
            result[org["id"]] = {
                "legal_name": org.get("legalName", ""),
                "country_name": oa_label or "",
                "country_code": oa_code or "",
                "country_source": "openaire",
                "ror": None,
            }
            rows_no_ror_with_country += 1
        else:
            rows_no_ror_no_country += 1

    if rows_read % LOG_EVERY == 0:
        print(f"  …{rows_read:,} organizations read, {rows_matched_ror:,} matched ROR so far")

# Write output JSON
with open(OUTPUT_JSON, "w", encoding="utf-8") as output_path:
    json.dump(result, output_path, indent=2, ensure_ascii=False)

# Record end time and calculate elapsed time
ended_at = datetime.now(timezone.utc)
elapsed_seconds = round(time.monotonic() - started_at, 2)
output_size_bytes = OUTPUT_JSON.stat().st_size

# Compile metadata about the processing run
metadata = {
    "elapsed_seconds": elapsed_seconds,
    "ended_at": ended_at.isoformat(),
    "rows_read": rows_read,
    "rows_matched_ror": rows_matched_ror,
    "rows_ror_not_in_dump": rows_ror_not_in_dump,
    "rows_no_ror_with_country": rows_no_ror_with_country,
    "rows_no_ror_no_country": rows_no_ror_no_country,
    "rows_in_output": len(result),
    "ror_index_size": len(ror_index),
    "output_json_size_bytes": output_size_bytes,
    "output_json_size_mb": round(output_size_bytes / 1024 / 1024, 2),
}

# Write metadata to JSON file
with OUTPUT_METADATA.open("w", encoding="utf-8") as metadata_path:
    json.dump(metadata, metadata_path, indent=2)

print(
    f"\n✔ output JSON written: {len(result):,} records -> "
    f"{OUTPUT_JSON.relative_to(DATA_DIR)}"
)

print(f"  {rows_matched_ror:,} matched via ROR")
print(f"  {rows_no_ror_with_country:,} no ROR id, country from OpenAIRE")
print(f"  {rows_ror_not_in_dump:,} had a ROR id not found in the ROR dump")
print(f"  {rows_no_ror_no_country:,} had no ROR id and no country in OpenAIRE")
print(f"Elapsed time: {elapsed_seconds} seconds")
print(f"Metadata written to: {OUTPUT_METADATA.relative_to(DATA_DIR)}")
