import csv
import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

# ==============================================================================
# CONSTANTS AND CONFIGURATION
# ==============================================================================

# Paths and directories
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"

IRIS_OC_PIDS_CSV_TEMPLATE = DATA_DIR / "iris_oc_pids" / "{university}" / "iris_oc_pids.csv"

OUTPUT_DIR = DATA_DIR / "top_cited"
OUTPUT_UNI_TEMPLATE = OUTPUT_DIR / "top_cited_{university}.csv"
OUTPUT_COMBINED = OUTPUT_DIR / "top_cited_combined.csv"
OUTPUT_METADATA = OUTPUT_DIR / "top_cited.metadata.json"

# Universities to process
UNIVERSITIES = ("SNS", "UNIBO", "UNIMI", "UNIPD", "UNITO", "UPO")

TOP_N = 10

# Progress logging
LOG_EVERY = 1_000_000

# Output CSV columns
FIELDNAMES = ["rank", "omid", "doi", "pmid", "isbn", "pub_date", "citation_count"]
FIELDNAMES_COMBINED = FIELDNAMES + ["universities"]


# ==============================================================================
# METHODS
# ==============================================================================

def format_elapsed(t0):
    """Format elapsed time since t0 (monotonic) as HhMMmSSs."""
    m, s = divmod(int(time.monotonic() - t0), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m{s:02d}s"


def write_top_csv(path, top_entries, omid_meta):
    """Write a top-cited CSV from a list of (omid, count) pairs."""
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        for rank, (omid, count) in enumerate(top_entries, 1):
            meta = omid_meta.get(omid, ("", "", "", ""))
            writer.writerow({
                "rank": rank,
                "omid": omid,
                "doi": meta[0],
                "pmid": meta[1],
                "isbn": meta[2],
                "pub_date": meta[3],
                "citation_count": count,
            })


# ==============================================================================
# RUNTIME
# ==============================================================================

if OUTPUT_COMBINED.exists():
    print(f"! output already exists, skipping: {OUTPUT_COMBINED.relative_to(DATA_DIR)}")
    sys.exit(0)

# Determine which universities have input data
universities_to_process = []
for university in UNIVERSITIES:
    input_csv = Path(str(IRIS_OC_PIDS_CSV_TEMPLATE).format(university=university))
    if not input_csv.exists():
        print(f"! input CSV not found for {university}, skipping")
        continue
    universities_to_process.append(university)

if not universities_to_process:
    print("Nothing to do.")
    raise SystemExit(0)

# ==============================================================================
# Phase 1 -- scan all CSVs, count incoming citations per cited OMID
# ==============================================================================

print("=" * 70)
print("Phase 1 -- scanning CSVs")
print("=" * 70)

uni_counters = {}
omid_meta = {}
csv_stats = {}

t0 = time.monotonic()

for university in universities_to_process:
    input_csv = Path(str(IRIS_OC_PIDS_CSV_TEMPLATE).format(university=university))
    print(f"  Scanning {input_csv.relative_to(DATA_DIR)} ...")

    counter = Counter()
    rows_read = 0

    with input_csv.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows_read += 1

            direction = row.get("direction", "").strip()
            if direction not in ("incoming", "internal"):
                continue

            cited = row.get("cited_omid", "").strip()
            if not cited:
                continue

            counter[cited] += 1

            if cited not in omid_meta:
                omid_meta[cited] = (
                    row.get("cited_doi", "").strip(),
                    row.get("cited_pmid", "").strip(),
                    row.get("cited_isbn", "").strip(),
                    row.get("cited_pub_date", "").strip(),
                )

            if rows_read % LOG_EVERY == 0:
                print(f"    {rows_read:,} rows | {format_elapsed(t0)}")

    uni_counters[university] = counter
    csv_stats[university] = rows_read
    print(f"    {rows_read:,} rows total, {len(counter):,} unique cited OMIDs")

# ==============================================================================
# Phase 2 -- compute combined ranking (max across universities per OMID)
# ==============================================================================

print()
print("=" * 70)
print("Phase 2 -- computing combined ranking")
print("=" * 70)

combined = Counter()
for counter in uni_counters.values():
    for omid, count in counter.items():
        if count > combined[omid]:
            combined[omid] = count

print(f"  {len(combined):,} unique cited OMIDs across all universities")

# ==============================================================================
# Phase 3 -- write output files
# ==============================================================================

print()
print("=" * 70)
print("Phase 3 -- writing output")
print("=" * 70)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

uni_metadata = {}

for university in universities_to_process:
    output_csv = Path(str(OUTPUT_UNI_TEMPLATE).format(university=university))
    top = uni_counters[university].most_common(TOP_N)
    write_top_csv(output_csv, top, omid_meta)

    uni_metadata[university] = {
        "university": university,
        "rows_read": csv_stats[university],
        "unique_cited_omids": len(uni_counters[university]),
        "top_cited": [{"omid": omid, "citation_count": count} for omid, count in top],
        "output_csv_size_bytes": output_csv.stat().st_size,
    }

    print(f"  {university}: wrote top {len(top)} to {output_csv.relative_to(DATA_DIR)}")
    for rank, (omid, count) in enumerate(top, 1):
        doi = omid_meta.get(omid, ("", "", "", ""))[0]
        label = doi if doi else omid
        print(f"    #{rank} {label} ({count:,} citations)")

# Combined (with universities column)
top_combined = combined.most_common(TOP_N)

with OUTPUT_COMBINED.open("w", encoding="utf-8", newline="\n") as fh:
    writer = csv.DictWriter(fh, fieldnames=FIELDNAMES_COMBINED, lineterminator="\n")
    writer.writeheader()
    for rank, (omid, count) in enumerate(top_combined, 1):
        meta = omid_meta.get(omid, ("", "", "", ""))
        unis = ";".join(u for u in UNIVERSITIES if omid in uni_counters.get(u, {}))
        writer.writerow({
            "rank": rank,
            "omid": omid,
            "doi": meta[0],
            "pmid": meta[1],
            "isbn": meta[2],
            "pub_date": meta[3],
            "citation_count": count,
            "universities": unis,
        })

print(f"  combined: wrote top {len(top_combined)} to {OUTPUT_COMBINED.relative_to(DATA_DIR)}")
for rank, (omid, count) in enumerate(top_combined, 1):
    doi = omid_meta.get(omid, ("", "", "", ""))[0]
    label = doi if doi else omid
    unis = ";".join(u for u in UNIVERSITIES if omid in uni_counters.get(u, {}))
    print(f"    #{rank} {label} ({count:,} citations) [{unis}]")

# Metadata
elapsed = time.monotonic() - t0
metadata = {
    "elapsed_seconds": round(elapsed, 2),
    "ended_at": datetime.now(timezone.utc).isoformat(),
    "top_n": TOP_N,
    "universities_processed": list(universities_to_process),
    "combined_unique_cited_omids": len(combined),
    "combined_top_cited": [{"omid": omid, "citation_count": count} for omid, count in top_combined],
    "per_university": uni_metadata,
}

with OUTPUT_METADATA.open("w", encoding="utf-8") as f:
    json.dump(metadata, f, indent=2)

print(f"\n  done [{format_elapsed(t0)}]")
