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

IRIS_DIR = DUMPS_DIR / "iris_publications"
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

# OC id fields can contain many space-separated identifiers and exceed the default limit
csv.field_size_limit(sys.maxsize)


# ==============================================================================
# METHODS
# ==============================================================================

def citation_direction(is_citing_iris, is_cited_iris):
    """Determine citation direction based on IRIS flags."""
    if is_citing_iris and is_cited_iris:
        return "internal"
    if is_citing_iris and not is_cited_iris:
        return "outgoing"
    if not is_citing_iris and is_cited_iris:
        return "incoming"
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
    # Avoid writing a duplicate header when appending to an existing file mid-run
    write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="\n") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


# ==============================================================================
# IDEMPOTENCY CHECK
# ==============================================================================

# Skip universities whose output CSV already exists; exit early if all are done
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
# None means "needed but not yet resolved"; replaced with (doi, pmid, isbn, pub_date) in Phase 2
needed_omids: dict[str, tuple | None] = {}
total_iris_rows = 0

# Collect citing and cited OMIDs from every university before touching the tar
for university in universities_to_process:
    index_csv = Path(str(INDEX_CSV_TEMPLATE).format(university=university))

    if not index_csv.exists():
        raise FileNotFoundError(f"IRIS index CSV not found: {index_csv}")

    # Track per-university row count for the per-university log line below
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

print(f"Streaming {OC_TAR_PATH.relative_to(DATA_DIR)}")
print(f"Looking for {remaining:,} OMIDs")

# Stream members one at a time to avoid extracting the whole ~350GB archive to disk
with tarfile.open(OC_TAR_PATH, "r:gz") as tar:
    for member in tar:
        # Skip directories and non-CSV entries inside the archive
        if not member.isfile() or not member.name.endswith(".csv"):
            continue

        # extractfile returns a file-like object; the member stays compressed on disk
        tar_files_scanned += 1
        fobj = tar.extractfile(member)
        if fobj is None:
            continue

        # Wrap binary stream for csv.reader, which requires a text-mode iterable
        text_stream = io.TextIOWrapper(fobj, encoding="utf-8", newline="")
        reader = csv.reader(text_stream)

        try:
            next(reader)
        except StopIteration:
            continue

        # Scan each row looking for OMIDs that are in our needed set
        for row in reader:
            tar_rows_scanned += 1

            # Periodic progress report since this phase can take hours
            if tar_rows_scanned % LOG_EVERY_TAR_ROWS == 0:
                elapsed = time.monotonic() - phase_2_start
                print(f"  {tar_rows_scanned:,} rows scanned, "
                      f"{omids_matched:,} matched, "
                      f"{remaining:,} remaining, "
                      f"{elapsed:.0f}s elapsed")

            # First column of each OC Meta row holds the space-separated identifier list
            id_field = row[0]
            match = OMID_RE.search(id_field)
            if not match:
                continue

            omid = match.group(0)
            # Skip OMIDs we don't need or have already resolved
            if omid not in needed_omids or needed_omids[omid] is not None:
                continue

            # Extract structured PIDs from the same field where we found the OMID
            doi, pmid, isbn = extract_pids(id_field)
            pub_date = row[7] if len(row) > 7 else None

            # Mark as resolved so it won't be matched again in a later archive member
            needed_omids[omid] = (doi, pmid, isbn, pub_date)
            omids_matched += 1
            remaining -= 1

            # Break both the inner row loop and the outer member loop
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
    # Collect only the non-empty PIDs this record carries
    present_pids = [
        (pid_type, record[pid_type])
        for pid_type in PID_TYPES
        if record[pid_type]
    ]

    # Nothing to deduplicate if the record carries no identifiers
    if not present_pids:
        return

    # Find any existing groups that share at least one PID with this record
    matching_indexes = {
        pid_to_group_index[(pid_type, pid_value)]
        for pid_type, pid_value in present_pids
        if (pid_type, pid_value) in pid_to_group_index
    }

    # No overlap with known groups — start a new group for this record
    if not matching_indexes:
        pid_groups.append(record)
        group_index = len(pid_groups) - 1
    else:
        # Merge all matching groups into the lowest-index one
        group_index = min(matching_indexes)

        # Iterate in reverse so each pop doesn't shift the indexes of later removals
        for other_index in sorted(matching_indexes - {group_index}, reverse=True):
            other_group = pid_groups[other_index]

            # Fill any gaps in the surviving group with PIDs from the absorbed group
            for pid_type in PID_TYPES:
                if not pid_groups[group_index][pid_type]:
                    pid_groups[group_index][pid_type] = other_group[pid_type]

            pid_groups.pop(other_index)

            # Drop entries for the absorbed group and shift all higher indexes down by one
            for pid_key in list(pid_to_group_index):
                idx = pid_to_group_index[pid_key]
                if idx == other_index:
                    del pid_to_group_index[pid_key]
                elif idx > other_index:
                    pid_to_group_index[pid_key] -= 1

        # Enrich the surviving group with any new PIDs the incoming record carries
        for pid_type in PID_TYPES:
            if record[pid_type] and not pid_groups[group_index][pid_type]:
                pid_groups[group_index][pid_type] = record[pid_type]

    # Register all PIDs in the final group so future records can find it by any of them
    for pid_type in PID_TYPES:
        pid_value = pid_groups[group_index][pid_type]
        if pid_value:
            pid_to_group_index[(pid_type, pid_value)] = group_index

# Re-read each university's IRIS CSV to build output rows with resolved metadata
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

    # Second pass over the same index file, now resolving metadata from Phase 2
    with index_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            rows_read += 1

            # Parse citing/cited OMIDs, OCI, and IRIS membership flags for this citation
            citing_omid = row["citing"].strip()
            cited_omid = row["cited"].strip()
            oci = row["id"].strip()
            is_citing_iris = row["is_citing_iris"].strip().lower() == "true"
            is_cited_iris = row["is_cited_iris"].strip().lower() == "true"

            direction = citation_direction(is_citing_iris, is_cited_iris)

            citing_meta = needed_omids.get(citing_omid)
            cited_meta = needed_omids.get(cited_omid)

            # OMIDs that weren't found in the OC dump go to the missing file for diagnostics
            if citing_meta is None or cited_meta is None:
                missing_side = []
                if citing_meta is None:
                    missing_side.append("citing")
                if cited_meta is None:
                    missing_side.append("cited")

                # Record which side(s) lacked metadata so callers can investigate
                missing_rows.append({
                    "oci": oci,
                    "direction": direction,
                    "missing_metadata": ";".join(missing_side),
                    "citing_omid": citing_omid,
                    "cited_omid": cited_omid,
                })

                rows_missing += 1

                # Flush buffer periodically to avoid unbounded memory growth
                if len(missing_rows) >= WRITE_CSV_EVERY:
                    append_csv_rows(missing_csv, missing_rows, MISSING_FIELDNAMES)
                    missing_rows.clear()

                continue

            # Unpack the (doi, pmid, isbn, pub_date) tuple stored for each OMID in Phase 2
            citing_doi, citing_pmid, citing_isbn, citing_pub_date = citing_meta
            cited_doi, cited_pmid, cited_isbn, cited_pub_date = cited_meta

            # Build a flat output row combining OCI, direction, and both sides' PIDs
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

            # Feed both citation endpoints into the union-find structure for unique_pids.csv
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

            # Flush buffer periodically to avoid unbounded memory growth
            if len(processed_rows) >= WRITE_CSV_EVERY:
                append_csv_rows(output_csv, processed_rows, PIDS_FIELDNAMES)
                processed_rows.clear()

            # Periodic progress log; IRIS CSVs can be many millions of rows
            if rows_read % LOG_EVERY_IRIS_ROWS == 0:
                print(f"  {university}: {rows_read:,} rows read, "
                      f"{rows_processed:,} processed, {rows_missing:,} missing")

    # Flush any rows still buffered after the loop ends
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

print(f"\nWriting {UNIQUE_PIDS_OUTPUT.relative_to(DATA_DIR)} with {len(pid_groups):,} unique PID groups")

UNIQUE_PIDS_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

# Sort groups so the output is stable and diffable across runs
with UNIQUE_PIDS_OUTPUT.open("w", encoding="utf-8", newline="\n") as f:
    writer = csv.DictWriter(f, fieldnames=PID_TYPES, lineterminator="\n")
    writer.writeheader()

    for group in sorted(
        pid_groups,
        key=lambda item: tuple(item[pid_type] for pid_type in PID_TYPES),
    ):
        writer.writerow(group)

# Capture output size and timing for the companion metadata file
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
