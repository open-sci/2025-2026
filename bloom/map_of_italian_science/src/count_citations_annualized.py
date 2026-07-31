import csv
import json
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import ijson

# ==============================================================================
# CONSTANTS AND CONFIGURATION
# ==============================================================================

# Paths and directories
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"

OMID_ORGANIZATIONS_JSON = DATA_DIR / "iris_openaire_organizations" / "omid_organizations.json"
IRIS_OC_PIDS_CSV_TEMPLATE = DATA_DIR / "iris_oc_pids" / "{university}" / "iris_oc_pids.csv"

OUTPUT_DIR = DATA_DIR / "citation_counts_annualized"
OUTPUT_ORG_INCOMING_TEMPLATE = OUTPUT_DIR / "{university}" / "{block}" / "citation_counts_organizations_incoming.csv"
OUTPUT_ORG_OUTGOING_TEMPLATE = OUTPUT_DIR / "{university}" / "{block}" / "citation_counts_organizations_outgoing.csv"
OUTPUT_COUNTRY_INCOMING_TEMPLATE = OUTPUT_DIR / "{university}" / "{block}" / "citation_counts_countries_incoming.csv"
OUTPUT_COUNTRY_OUTGOING_TEMPLATE = OUTPUT_DIR / "{university}" / "{block}" / "citation_counts_countries_outgoing.csv"
OUTPUT_METADATA_TEMPLATE = OUTPUT_DIR / "{university}" / "citation_counts.metadata.json"

# Universities to process
UNIVERSITIES = ("SNS", "UNIBO", "UNIMI", "UNIPD", "UNITO", "UPO")

# 5-year publication-year blocks, starting from 2001 (2026+ and pre-2001 excluded).
YEAR_BLOCKS = [
    (2001, 2005, "2001-2005"),
    (2006, 2010, "2006-2010"),
    (2011, 2015, "2011-2015"),
    (2016, 2020, "2016-2020"),
    (2021, 2025, "2021-2025"),
]
BLOCK_LABELS = [label for _, _, label in YEAR_BLOCKS]

# Progress logging
LOG_EVERY_CSV = 1_000_000
LOG_EVERY_JSON = 1_000_000


# ==============================================================================
# METHODS
# ==============================================================================

def format_elapsed(t0):
    """Format elapsed time since t0 (monotonic) as HhMMmSSs."""
    m, s = divmod(int(time.monotonic() - t0), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m{s:02d}s"


def year_to_block(pub_date):
    """Map a pub_date string (YYYY, YYYY-MM, or YYYY-MM-DD) to its 5-year block label.

    Returns None for blank, unparseable, pre-2000, or 2026+ dates, which are then
    excluded from all blocks.
    """
    s = (pub_date or "").strip()
    if len(s) < 4 or not s[:4].isdigit():
        return None
    year = int(s[:4])
    for lo, hi, label in YEAR_BLOCKS:
        if lo <= year <= hi:
            return label
    return None


def dedup_org_list(organizations):
    """Deduplicate a per-OMID organizations list by ROR id.

    The ROR id is the organization's identity, so no name comparison is needed —
    entries carrying the same ROR id are the same organization.
    """
    by_ror = {}
    for org in organizations:
        ror = org.get("ror") or ""
        if ror and ror not in by_ror:
            by_ror[ror] = org

    return list(by_ror.values())


def write_org_csv(path, counter):
    """Write organization counts CSV sorted by count descending."""
    fieldnames = ["legal_name", "country_name", "country_code", "ror", "count"]
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for key, count in sorted(counter.items(), key=lambda x: -x[1]):
            legal_name, country_name, country_code, ror = key
            writer.writerow({
                "legal_name": legal_name,
                "country_name": country_name,
                "country_code": country_code,
                "ror": ror,
                "count": count,
            })


def write_country_csv(path, counter):
    """Write country counts CSV sorted by count descending."""
    fieldnames = ["country_name", "country_code", "count"]
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, lineterminator="\n")
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
    output_check = OUTPUT_DIR / university
    input_csv = Path(str(IRIS_OC_PIDS_CSV_TEMPLATE).format(university=university))
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
# Phase 1 -- scan all CSVs, build omid -> {(university, direction, block): count}
# ==============================================================================

print("=" * 70)
print("Phase 1 -- scanning CSVs")
print("=" * 70)

# For each omid, how many times it contributes to each (university, direction, block).
# The block is taken from the pub_date of the same paper whose organizations are counted:
# incoming uses the citing paper's date, outgoing uses the cited paper's date.
omid_contributions = defaultdict(Counter)
csv_stats = {}

t0 = time.monotonic()

for university in universities_to_process:
    input_csv = Path(str(IRIS_OC_PIDS_CSV_TEMPLATE).format(university=university))
    print(f"  Scanning {input_csv.relative_to(DATA_DIR)} ...")

    rows_read = 0
    with input_csv.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows_read += 1

            direction = row.get("direction", "").strip()
            citing = row.get("citing_omid", "").strip()
            cited = row.get("cited_omid", "").strip()
            citing_block = year_to_block(row.get("citing_pub_date", ""))
            cited_block = year_to_block(row.get("cited_pub_date", ""))

            if direction == "incoming":
                if citing_block:
                    omid_contributions[citing][(university, "incoming", citing_block)] += 1
            elif direction == "outgoing":
                if cited_block:
                    omid_contributions[cited][(university, "outgoing", cited_block)] += 1
            elif direction == "internal":
                if cited_block:
                    omid_contributions[cited][(university, "incoming", cited_block)] += 1
                if citing_block:
                    omid_contributions[citing][(university, "outgoing", citing_block)] += 1

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

# Output counters keyed by (university, block): {(university, block): Counter}
org_incoming = defaultdict(Counter)
org_outgoing = defaultdict(Counter)
country_incoming = defaultdict(Counter)
country_outgoing = defaultdict(Counter)

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
        organizations = dedup_org_list(organizations)

        for (university, direction, block), multiplier in omid_contributions[omid].items():
            oc = org_incoming if direction == "incoming" else org_outgoing
            cc = country_incoming if direction == "incoming" else country_outgoing

            for org in organizations:
                org_key = (
                    org.get("legal_name", ""),
                    org.get("country_name", ""),
                    org.get("country_code", ""),
                    org.get("ror") or "",
                )
                oc[(university, block)][org_key] += multiplier

                country_key = (
                    org.get("country_name", ""),
                    org.get("country_code", ""),
                )
                cc[(university, block)][country_key] += multiplier

        del omid_contributions[omid]

print(f"  {scanned:,} entries scanned, {matched:,} matched, "
      f"{matched_with_orgs:,} with organizations | {format_elapsed(t0)}")

not_in_json = len(omid_contributions)
if not_in_json:
    print(f"  {not_in_json:,} omids from CSVs not found in JSON")

# No post-hoc merging: every counter key is already identified by its ROR id

# ==============================================================================
# Phase 3 -- write output files
# ==============================================================================

print()
print("=" * 70)
print("Phase 3 -- writing output")
print("=" * 70)


def file_size(p):
    return p.stat().st_size if p.exists() else 0


for university in universities_to_process:
    metadata_json = Path(str(OUTPUT_METADATA_TEMPLATE).format(university=university))
    metadata_json.parent.mkdir(parents=True, exist_ok=True)

    blocks_meta = {}

    for block in BLOCK_LABELS:
        key = (university, block)

        output_org_in = Path(str(OUTPUT_ORG_INCOMING_TEMPLATE).format(university=university, block=block))
        output_org_out = Path(str(OUTPUT_ORG_OUTGOING_TEMPLATE).format(university=university, block=block))
        output_country_in = Path(str(OUTPUT_COUNTRY_INCOMING_TEMPLATE).format(university=university, block=block))
        output_country_out = Path(str(OUTPUT_COUNTRY_OUTGOING_TEMPLATE).format(university=university, block=block))

        output_org_in.parent.mkdir(parents=True, exist_ok=True)

        write_org_csv(output_org_in, org_incoming[key])
        write_org_csv(output_org_out, org_outgoing[key])
        write_country_csv(output_country_in, country_incoming[key])
        write_country_csv(output_country_out, country_outgoing[key])

        blocks_meta[block] = {
            "unique_organizations_incoming": len(org_incoming[key]),
            "unique_organizations_outgoing": len(org_outgoing[key]),
            "unique_countries_incoming": len(country_incoming[key]),
            "unique_countries_outgoing": len(country_outgoing[key]),
            "output_org_incoming_csv_size_bytes": file_size(output_org_in),
            "output_org_outgoing_csv_size_bytes": file_size(output_org_out),
            "output_country_incoming_csv_size_bytes": file_size(output_country_in),
            "output_country_outgoing_csv_size_bytes": file_size(output_country_out),
        }

    metadata = {
        "university": university,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "rows_read": csv_stats[university],
        "year_blocks": BLOCK_LABELS,
        "blocks": blocks_meta,
    }

    with metadata_json.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    summary = ", ".join(
        f"{block} in {blocks_meta[block]['unique_countries_incoming']}c/"
        f"out {blocks_meta[block]['unique_countries_outgoing']}c"
        for block in BLOCK_LABELS
    )
    print(f"  {university}: {summary}")
