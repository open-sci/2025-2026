import csv
import io
import json
import re
import sys
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path

# ==============================================================================
# CONSTANTS AND CONFIGURATION
# ==============================================================================

# Paths and directories
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
DUMPS_DIR = DATA_DIR / "dumps"

IRIS_DIR = DUMPS_DIR / "iris"
OC_TAR_PATH = DUMPS_DIR / "opencitations" / "output_csv_2026_01_14.tar.gz"

INDEX_CSV_TEMPLATE = IRIS_DIR / "{university}" / "iris_in_oc_index" / "iris_in_oc_index.csv"

OUTPUT_DIR = DATA_DIR / "iris_oc_pids"
UNIQUE_PIDS_OUTPUT = OUTPUT_DIR / "unique_pids.csv"
OUTPUT_PIDS_TEMPLATE = OUTPUT_DIR / "{university}" / "iris_oc_pids.csv"
OUTPUT_MISSING_TEMPLATE = OUTPUT_DIR / "{university}" / "iris_oc_pids.missing.csv"
OUTPUT_METADATA_TEMPLATE = OUTPUT_DIR / "{university}" / "iris_oc_pids.metadata.json"

# Configurations
OMID_RE = re.compile(r"\bomid:[^\s\]]+")

WRITE_CSV_EVERY = 5000
LOG_EVERY_TAR_ROWS = 5_000_000
LOG_EVERY_IRIS_ROWS = 1_000_000

IRIS_UNIVERSITIES = ("SNS", "UNIBO", "UNIMI", "UNIPD", "UNITO", "UPO")

PID_TYPES = ["omid", "doi", "pmid", "isbn"]
PIDS_FIELDNAMES = [
    "oci", "direction",
    "citing_omid", "citing_doi", "citing_pmid", "citing_isbn", "citing_pub_date",
    "cited_omid", "cited_doi", "cited_pmid", "cited_isbn", "cited_pub_date",
]
MISSING_FIELDNAMES = [
    "oci", "direction", "missing_metadata", "citing_omid", "cited_omid",
]

csv.field_size_limit(sys.maxsize)


# ==============================================================================
# METHODS
# ==============================================================================

def citation_direction(is_citing_iris, is_cited_iris):
    """Determine citation direction based on IRIS flags."""
    if is_citing_iris and is_cited_iris:
        return "internal"
    if is_citing_iris and not is_cited_iris:
        return "outbound"
    if not is_citing_iris and is_cited_iris:
        return "inbound"
    raise ValueError("Both is_citing_iris and is_cited_iris are False")


def extract_pids(id_field):
    """Extract DOI, PMID, ISBN from a space-separated identifier field."""
    pids = {}
    for tok in id_field.split():
        if tok.startswith(("doi:", "pmid:", "isbn:")):
            prefix = tok.split(":", 1)[0]
            pids[prefix] = tok
    return pids.get("doi"), pids.get("pmid"), pids.get("isbn")


def append_csv_rows(path, rows, fieldnames):
    """Append rows to a CSV file, writing header only if file doesn't exist."""
    if not rows:
        return
    write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="\n") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def format_path(path):
    """Format a path relative to DATA_DIR for display."""
    try:
        return str(path.relative_to(DATA_DIR))
    except ValueError:
        return str(path)


# ==============================================================================
# IDEMPOTENCY CHECK
# ==============================================================================

universities_to_process = []
for university in IRIS_UNIVERSITIES:
    output_csv = Path(str(OUTPUT_PIDS_TEMPLATE).format(university=university))
    if output_csv.exists():
        print(f"! output already exists for {university}, skipping")
        continue
    universities_to_process.append(university)

if not universities_to_process and UNIQUE_PIDS_OUTPUT.exists():
    print("All output files already exist, skipping.")
    sys.exit(0)


# ==============================================================================
# PHASE 1 — Collect needed OMIDs from IRIS CSVs
# ==============================================================================

print("=" * 70)
print("PHASE 1 — Collecting needed OMIDs from IRIS CSVs")
print("=" * 70)

phase_1_start = time.monotonic()
needed_omids: dict[str, tuple | None] = {}
total_iris_rows = 0

for university in universities_to_process:
    index_csv = Path(str(INDEX_CSV_TEMPLATE).format(university=university))

    if not index_csv.exists():
        raise FileNotFoundError(f"IRIS index CSV not found: {index_csv}")

    uni_rows = 0
    with index_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            citing = row["citing"].strip()
            cited = row["cited"].strip()
            if citing not in needed_omids:
                needed_omids[citing] = None
            if cited not in needed_omids:
                needed_omids[cited] = None
            uni_rows += 1
            total_iris_rows += 1

    print(f"  {university}: {uni_rows:,} rows")

phase_1_elapsed = time.monotonic() - phase_1_start
print(f"Collected {len(needed_omids):,} unique OMIDs from {total_iris_rows:,} IRIS rows "
      f"in {phase_1_elapsed:.1f}s")


# ==============================================================================
# PHASE 2 — Stream OpenCitations tar.gz, extract metadata for needed OMIDs
# ==============================================================================

print()
print("=" * 70)
print("PHASE 2 — Streaming OpenCitations tar.gz")
print("=" * 70)

if not OC_TAR_PATH.exists():
    raise FileNotFoundError(f"OpenCitations tar.gz not found: {OC_TAR_PATH}")

phase_2_start = time.monotonic()
remaining = sum(1 for v in needed_omids.values() if v is None)
tar_rows_scanned = 0
tar_files_scanned = 0
omids_matched = 0

print(f"Streaming {format_path(OC_TAR_PATH)}")
print(f"Looking for {remaining:,} OMIDs")

with tarfile.open(OC_TAR_PATH, "r:gz") as tar:
    for member in tar:
        if not member.isfile() or not member.name.endswith(".csv"):
            continue

        tar_files_scanned += 1
        fobj = tar.extractfile(member)
        if fobj is None:
            continue

        text_stream = io.TextIOWrapper(fobj, encoding="utf-8", newline="")
        reader = csv.reader(text_stream)

        try:
            next(reader)
        except StopIteration:
            continue

        for row in reader:
            tar_rows_scanned += 1

            if tar_rows_scanned % LOG_EVERY_TAR_ROWS == 0:
                elapsed = time.monotonic() - phase_2_start
                print(f"  {tar_rows_scanned:,} rows scanned, "
                      f"{omids_matched:,} matched, "
                      f"{remaining:,} remaining, "
                      f"{elapsed:.0f}s elapsed")

            id_field = row[0]
            match = OMID_RE.search(id_field)
            if not match:
                continue

            omid = match.group(0)
            if omid not in needed_omids or needed_omids[omid] is not None:
                continue

            doi, pmid, isbn = extract_pids(id_field)
            pub_date = row[7] if len(row) > 7 else None

            needed_omids[omid] = (doi, pmid, isbn, pub_date)
            omids_matched += 1
            remaining -= 1

            if remaining == 0:
                print(f"  All OMIDs matched after {tar_rows_scanned:,} rows — stopping early")
                break

        if remaining == 0:
            break

phase_2_elapsed = time.monotonic() - phase_2_start
unmatched = sum(1 for v in needed_omids.values() if v is None)
print(f"Scanned {tar_rows_scanned:,} rows from {tar_files_scanned:,} CSV files "
      f"in {phase_2_elapsed:.0f}s")
print(f"Matched {omids_matched:,} OMIDs, {unmatched:,} unmatched")


# ==============================================================================
# PHASE 3 — Build iris_oc_pids CSVs and unique_pids.csv
# ==============================================================================

print()
print("=" * 70)
print("PHASE 3 — Building output CSVs")
print("=" * 70)

phase_3_start = time.monotonic()
OUTPUT_DIR.mkdir(exist_ok=True)

pid_groups = []
pid_to_group_index = {}


def register_for_dedup(record):
    """Register a publication record with the union-find deduplication structure."""
    global pid_to_group_index

    present_pids = [
        (pid_type, record[pid_type])
        for pid_type in PID_TYPES
        if record[pid_type]
    ]

    if not present_pids:
        return

    matching_indexes = {
        pid_to_group_index[(pid_type, pid_value)]
        for pid_type, pid_value in present_pids
        if (pid_type, pid_value) in pid_to_group_index
    }

    if not matching_indexes:
        pid_groups.append(record)
        group_index = len(pid_groups) - 1
    else:
        group_index = min(matching_indexes)

        for other_index in sorted(matching_indexes - {group_index}, reverse=True):
            other_group = pid_groups[other_index]

            for pid_type in PID_TYPES:
                if not pid_groups[group_index][pid_type]:
                    pid_groups[group_index][pid_type] = other_group[pid_type]

            pid_groups.pop(other_index)

            pid_to_group_index = {
                pid_key: index - 1 if index > other_index else index
                for pid_key, index in pid_to_group_index.items()
                if index != other_index
            }

        for pid_type in PID_TYPES:
            if record[pid_type] and not pid_groups[group_index][pid_type]:
                pid_groups[group_index][pid_type] = record[pid_type]

    for pid_type in PID_TYPES:
        pid_value = pid_groups[group_index][pid_type]
        if pid_value:
            pid_to_group_index[(pid_type, pid_value)] = group_index


for university in universities_to_process:
    index_csv = Path(str(INDEX_CSV_TEMPLATE).format(university=university))
    output_csv = Path(str(OUTPUT_PIDS_TEMPLATE).format(university=university))
    missing_csv = Path(str(OUTPUT_MISSING_TEMPLATE).format(university=university))
    metadata_json = Path(str(OUTPUT_METADATA_TEMPLATE).format(university=university))

    output_csv.parent.mkdir(exist_ok=True)

    print(f"\nProcessing {university}")
    uni_start = time.monotonic()

    processed_rows = []
    missing_rows = []
    rows_read = 0
    rows_processed = 0
    rows_missing = 0

    with index_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            rows_read += 1

            citing_omid = row["citing"].strip()
            cited_omid = row["cited"].strip()
            oci = row["id"].strip()
            is_citing_iris = row["is_citing_iris"].strip().lower() == "true"
            is_cited_iris = row["is_cited_iris"].strip().lower() == "true"

            direction = citation_direction(is_citing_iris, is_cited_iris)

            citing_meta = needed_omids.get(citing_omid)
            cited_meta = needed_omids.get(cited_omid)

            if citing_meta is None or cited_meta is None:
                missing_side = []
                if citing_meta is None:
                    missing_side.append("citing")
                if cited_meta is None:
                    missing_side.append("cited")

                missing_rows.append({
                    "oci": oci,
                    "direction": direction,
                    "missing_metadata": ";".join(missing_side),
                    "citing_omid": citing_omid,
                    "cited_omid": cited_omid,
                })

                rows_missing += 1

                if len(missing_rows) >= WRITE_CSV_EVERY:
                    append_csv_rows(missing_csv, missing_rows, MISSING_FIELDNAMES)
                    missing_rows.clear()

                continue

            citing_doi, citing_pmid, citing_isbn, citing_pub_date = citing_meta
            cited_doi, cited_pmid, cited_isbn, cited_pub_date = cited_meta

            processed_rows.append({
                "oci": oci,
                "direction": direction,
                "citing_omid": citing_omid,
                "citing_doi": citing_doi or "",
                "citing_pmid": citing_pmid or "",
                "citing_isbn": citing_isbn or "",
                "citing_pub_date": citing_pub_date or "",
                "cited_omid": cited_omid,
                "cited_doi": cited_doi or "",
                "cited_pmid": cited_pmid or "",
                "cited_isbn": cited_isbn or "",
                "cited_pub_date": cited_pub_date or "",
            })

            register_for_dedup({
                "omid": citing_omid,
                "doi": citing_doi or "",
                "pmid": citing_pmid or "",
                "isbn": citing_isbn or "",
            })
            register_for_dedup({
                "omid": cited_omid,
                "doi": cited_doi or "",
                "pmid": cited_pmid or "",
                "isbn": cited_isbn or "",
            })

            rows_processed += 1

            if len(processed_rows) >= WRITE_CSV_EVERY:
                append_csv_rows(output_csv, processed_rows, PIDS_FIELDNAMES)
                processed_rows.clear()

            if rows_read % LOG_EVERY_IRIS_ROWS == 0:
                print(f"  {university}: {rows_read:,} rows read, "
                      f"{rows_processed:,} processed, {rows_missing:,} missing")

    append_csv_rows(output_csv, processed_rows, PIDS_FIELDNAMES)
    append_csv_rows(missing_csv, missing_rows, MISSING_FIELDNAMES)

    uni_elapsed = round(time.monotonic() - uni_start, 2)
    output_size = output_csv.stat().st_size if output_csv.exists() else 0
    missing_size = missing_csv.stat().st_size if missing_csv.exists() else 0

    metadata = {
        "university": university,
        "elapsed_seconds": uni_elapsed,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "rows_read": rows_read,
        "rows_processed": rows_processed,
        "rows_missing_metadata": rows_missing,
        "write_csv_every": WRITE_CSV_EVERY,
        "output_csv_size_bytes": output_size,
        "output_csv_size_mb": round(output_size / 1024 / 1024, 2),
        "missing_pids_csv_size_bytes": missing_size,
        "missing_pids_csv_size_mb": round(missing_size / 1024 / 1024, 2),
    }

    with metadata_json.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print(f"  {university}: {rows_processed:,} processed, {rows_missing:,} missing "
          f"in {uni_elapsed:.1f}s")


# ==============================================================================
# WRITE unique_pids.csv
# ==============================================================================

print(f"\nWriting {format_path(UNIQUE_PIDS_OUTPUT)} with {len(pid_groups):,} unique PID groups")

UNIQUE_PIDS_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

with UNIQUE_PIDS_OUTPUT.open("w", encoding="utf-8", newline="\n") as f:
    writer = csv.DictWriter(f, fieldnames=PID_TYPES, lineterminator="\n")
    writer.writeheader()

    for group in sorted(
        pid_groups,
        key=lambda item: tuple(item[pid_type] for pid_type in PID_TYPES),
    ):
        writer.writerow(group)

unique_pids_size = UNIQUE_PIDS_OUTPUT.stat().st_size if UNIQUE_PIDS_OUTPUT.exists() else 0
unique_pids_metadata = {
    "elapsed_seconds": round(time.monotonic() - phase_3_start, 2),
    "ended_at": datetime.now(timezone.utc).isoformat(),
    "unique_pid_groups": len(pid_groups),
    "output_csv_size_bytes": unique_pids_size,
    "output_csv_size_mb": round(unique_pids_size / 1024 / 1024, 2),
}

with UNIQUE_PIDS_OUTPUT.with_suffix(".metadata.json").open("w", encoding="utf-8") as f:
    json.dump(unique_pids_metadata, f, indent=2)


# ==============================================================================
# FINAL SUMMARY
# ==============================================================================

total_elapsed = round(time.monotonic() - phase_1_start, 2)

print()
print("=" * 70)
print("DONE")
print("=" * 70)
print(f"Phase 1 (collect OMIDs): {phase_1_elapsed:.1f}s")
print(f"Phase 2 (stream tar.gz): {phase_2_elapsed:.0f}s — "
      f"{tar_rows_scanned:,} rows, {omids_matched:,} matched")
print(f"Phase 3 (build outputs): {time.monotonic() - phase_3_start:.1f}s")
print(f"Total elapsed: {total_elapsed:.1f}s")
print(f"Unique PID groups: {len(pid_groups):,}")
