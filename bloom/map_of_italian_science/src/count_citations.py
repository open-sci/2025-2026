import csv
import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import ijson


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
IRIS_OC_PIDS_DIR = DATA_DIR / "iris_oc_pids"
OMID_ORGANIZATIONS_JSON = DATA_DIR / "openaire_organizations" / "omid_organizations.json"
OUTPUT_DIR = DATA_DIR / "citation_counts"

# File templates
INPUT_CSV_TEMPLATE = IRIS_OC_PIDS_DIR / "{university}" / "iris_oc_pids.csv"
OUTPUT_ORG_INBOUND_TEMPLATE = OUTPUT_DIR / "{university}" / "citation_counts_organizations_inbound.csv"
OUTPUT_ORG_OUTBOUND_TEMPLATE = OUTPUT_DIR / "{university}" / "citation_counts_organizations_outbound.csv"
OUTPUT_COUNTRY_INBOUND_TEMPLATE = OUTPUT_DIR / "{university}" / "citation_counts_countries_inbound.csv"
OUTPUT_COUNTRY_OUTBOUND_TEMPLATE = OUTPUT_DIR / "{university}" / "citation_counts_countries_outbound.csv"
OUTPUT_METADATA_TEMPLATE = OUTPUT_DIR / "{university}" / "citation_counts.metadata.json"

# Universities to process
UNIVERSITIES = ("SNS", "UNIBO", "UNIMI", "UNIPD", "UNITO", "UPO")

# Progress logging
LOG_EVERY_CSV = 100_000
LOG_EVERY_JSON = 1_000_000


# ==============================================================================
# METHODS
# ==============================================================================

def format_elapsed(t0):
    """Format elapsed time since t0 (monotonic) as HhMMmSSs."""
    m, s = divmod(int(time.monotonic() - t0), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m{s:02d}s"


def write_org_csv(path, counter):
    """Write organization counts CSV sorted by count descending."""
    fieldnames = ["legal_name", "country_name", "country_code",
                  "ror", "openaire", "count"]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for key, count in sorted(counter.items(), key=lambda x: -x[1]):
            legal_name, country_name, country_code, ror, openaire = key
            writer.writerow({
                "legal_name": legal_name,
                "country_name": country_name,
                "country_code": country_code,
                "ror": ror,
                "openaire": openaire,
                "count": count,
            })


def write_country_csv(path, counter):
    """Write country counts CSV sorted by count descending."""
    fieldnames = ["country_name", "country_code", "count"]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for key, count in sorted(counter.items(), key=lambda x: -x[1]):
            country_name, country_code = key
            writer.writerow({
                "country_name": country_name,
                "country_code": country_code,
                "count": count,
            })


# ==============================================================================
# RUNTIME
# ==============================================================================

# Create output directory
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Determine which universities still need processing
universities_to_process = []
for university in UNIVERSITIES:
    output_check = Path(str(OUTPUT_ORG_INBOUND_TEMPLATE).format(university=university))
    input_csv = Path(str(INPUT_CSV_TEMPLATE).format(university=university))
    if output_check.exists():
        print(f"! output already exists for {university}, skipping")
        continue
    if not input_csv.exists():
        print(f"! input CSV not found for {university}, skipping")
        continue
    universities_to_process.append(university)

if not universities_to_process:
    print("Nothing to do.")
    raise SystemExit(0)

# ==============================================================================
# Phase 1 -- scan all CSVs, build omid -> {(university, direction): count}
# ==============================================================================

print("=" * 70)
print("Phase 1 -- scanning CSVs")
print("=" * 70)

# For each omid, how many times it contributes to each (university, direction)
omid_contributions = defaultdict(Counter)
csv_stats = {}

t0 = time.monotonic()

for university in universities_to_process:
    input_csv = Path(str(INPUT_CSV_TEMPLATE).format(university=university))
    print(f"  Scanning {input_csv.relative_to(DATA_DIR)} ...")

    rows_read = 0
    with input_csv.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows_read += 1

            direction = row.get("direction", "").strip()
            citing = row.get("citing_omid", "").strip()
            cited = row.get("cited_omid", "").strip()

            if direction == "inbound":
                omid_contributions[citing][(university, "inbound")] += 1
            elif direction == "outbound":
                omid_contributions[cited][(university, "outbound")] += 1
            elif direction == "internal":
                omid_contributions[cited][(university, "inbound")] += 1
                omid_contributions[citing][(university, "outbound")] += 1

            if rows_read % LOG_EVERY_CSV == 0:
                print(f"    {rows_read:,} rows | {format_elapsed(t0)}")

    csv_stats[university] = rows_read
    print(f"    {rows_read:,} rows total")

print(f"  {len(omid_contributions):,} unique omids to look up | {format_elapsed(t0)}")

# ==============================================================================
# Phase 2 -- stream JSON, directly increment output counters
# ==============================================================================

print()
print("=" * 70)
print("Phase 2 -- streaming omid_organizations.json")
print("=" * 70)

# Output counters: {university: Counter}
org_inbound = defaultdict(Counter)
org_outbound = defaultdict(Counter)
country_inbound = defaultdict(Counter)
country_outbound = defaultdict(Counter)

scanned = 0
matched = 0
matched_with_orgs = 0

t0 = time.monotonic()

print(f"  Streaming {OMID_ORGANIZATIONS_JSON.relative_to(DATA_DIR)} ...")

with OMID_ORGANIZATIONS_JSON.open("rb") as fh:
    for omid, entry in ijson.kvitems(fh, ""):
        scanned += 1
        if scanned % LOG_EVERY_JSON == 0:
            print(f"  ...{scanned:,} entries scanned, {matched_with_orgs:,} matched | "
                  f"{format_elapsed(t0)}")

        if omid not in omid_contributions:
            continue

        matched += 1
        organizations = entry.get("organizations", [])
        if not organizations:
            continue

        matched_with_orgs += 1

        for (university, direction), multiplier in omid_contributions[omid].items():
            oc = org_inbound if direction == "inbound" else org_outbound
            cc = country_inbound if direction == "inbound" else country_outbound

            for org in organizations:
                org_key = (
                    org.get("legal_name", ""),
                    org.get("country_name", ""),
                    org.get("country_code", ""),
                    org.get("ror") or "",
                    org.get("openaire", ""),
                )
                oc[university][org_key] += multiplier

                country_key = (
                    org.get("country_name", ""),
                    org.get("country_code", ""),
                )
                cc[university][country_key] += multiplier

        del omid_contributions[omid]

print(f"  {scanned:,} entries scanned, {matched:,} matched, "
      f"{matched_with_orgs:,} with organizations | {format_elapsed(t0)}")

not_in_json = len(omid_contributions)
if not_in_json:
    print(f"  {not_in_json:,} omids from CSVs not found in JSON")

# ==============================================================================
# Phase 3 -- write output files
# ==============================================================================

print()
print("=" * 70)
print("Phase 3 -- writing output")
print("=" * 70)

for university in universities_to_process:
    output_org_in = Path(str(OUTPUT_ORG_INBOUND_TEMPLATE).format(university=university))
    output_org_out = Path(str(OUTPUT_ORG_OUTBOUND_TEMPLATE).format(university=university))
    output_country_in = Path(str(OUTPUT_COUNTRY_INBOUND_TEMPLATE).format(university=university))
    output_country_out = Path(str(OUTPUT_COUNTRY_OUTBOUND_TEMPLATE).format(university=university))
    metadata_json = Path(str(OUTPUT_METADATA_TEMPLATE).format(university=university))

    output_org_in.parent.mkdir(parents=True, exist_ok=True)

    write_org_csv(output_org_in, org_inbound[university])
    write_org_csv(output_org_out, org_outbound[university])
    write_country_csv(output_country_in, country_inbound[university])
    write_country_csv(output_country_out, country_outbound[university])

    def file_size(p):
        return p.stat().st_size if p.exists() else 0

    metadata = {
        "university": university,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "rows_read": csv_stats[university],
        "unique_organizations_inbound": len(org_inbound[university]),
        "unique_organizations_outbound": len(org_outbound[university]),
        "unique_countries_inbound": len(country_inbound[university]),
        "unique_countries_outbound": len(country_outbound[university]),
        "output_org_inbound_csv_size_bytes": file_size(output_org_in),
        "output_org_outbound_csv_size_bytes": file_size(output_org_out),
        "output_country_inbound_csv_size_bytes": file_size(output_country_in),
        "output_country_outbound_csv_size_bytes": file_size(output_country_out),
    }

    with metadata_json.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print(f"  {university}: inbound {len(org_inbound[university]):,} orgs / "
          f"{len(country_inbound[university]):,} countries, "
          f"outbound {len(org_outbound[university]):,} orgs / "
          f"{len(country_outbound[university]):,} countries")
