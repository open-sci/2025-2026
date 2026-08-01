import csv
import gzip
import json
import os
import re
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from glob import glob
from pathlib import Path

# ==============================================================================
# CONSTANTS AND CONFIGURATION
# ==============================================================================

# Paths and directories
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"

DUMPS_DIR = DATA_DIR / "dumps"
OPENAIRE_DIR = DUMPS_DIR / "openaire"
PUBLICATION_TAR_PATTERN = "publication_*.tar"
RELATION_TAR_PATTERN = "relation_*.tar"
ROR_ORGANIZATIONS_JSON = DATA_DIR / "openaire_ror_countries" / "ror_organizations.json"
OPENAIRE_ROR_MAP_JSON = DATA_DIR / "openaire_ror_countries" / "openaire_ror_map.json"

UNIQUE_PIDS_CSV = DATA_DIR / "iris_oc_pids" / "unique_pids.csv"

OUTPUT_DIR = DATA_DIR / "iris_openaire_organizations"
OUTPUT_JSON = OUTPUT_DIR / "omid_organizations.json"
MISSING_CSV = OUTPUT_DIR / "missing_no_searchable_pid.csv"
OUTPUT_METADATA = OUTPUT_DIR / "omid_organizations.metadata.json"

CHECKPOINT_PHASE1 = OUTPUT_DIR / "_checkpoint_phase1.json"
CHECKPOINT_PHASE2 = OUTPUT_DIR / "_checkpoint_phase2.json"

# Configurations
LOG_EVERY_PUBLICATIONS = 1_000_000
LOG_EVERY_RELATIONS = 5_000_000

FLUSH_EVERY = 50_000

_DOI_PREFIX_RE = re.compile(r"^(https?://)?(dx\.)?doi\.org/", re.IGNORECASE)
_ENTITY_PREFIX_RE = re.compile(r"^\d+\|")

AFFILIATION_RELS = {"hasauthorinstitution", "isauthorinstitutionof"}


# ==============================================================================
# METHODS — normalization
# ==============================================================================

def normalize_doi(raw):
    """Lowercase DOI, strip resolver prefix and 'doi:' scheme prefix."""
    if not raw:
        return None
    raw = raw.strip()
    if raw.lower().startswith("doi:"):
        raw = raw[4:]
    raw = _DOI_PREFIX_RE.sub("", raw)
    return raw.lower().strip() or None


def normalize_pmid(raw):
    """Strip 'pmid:' scheme prefix."""
    if not raw:
        return None
    raw = raw.strip()
    if raw.lower().startswith("pmid:"):
        raw = raw[5:]
    return raw.strip() or None


def strip_entity_prefix(eid):
    """Remove leading 'NN|' OpenAIRE entity-type prefix if present."""
    if eid is None:
        return None
    return _ENTITY_PREFIX_RE.sub("", eid.strip())


# ==============================================================================
# METHODS — tar streaming
# ==============================================================================

def iter_tar_records(tar_path):
    """Yield parsed JSON objects from a *.tar of *.json.gz members.

    Fully streaming: never holds more than one line in memory. Malformed lines
    are silently skipped.
    """
    with tarfile.open(tar_path, "r") as tar:
        for member in tar:
            if not member.isfile():
                continue
            base = os.path.basename(member.name)
            if base.startswith("."):
                continue
            name_lower = member.name.lower()
            if not (name_lower.endswith(".gz") or name_lower.endswith(".json")):
                continue
            fobj = tar.extractfile(member)
            if fobj is None:
                continue
            stream = (
                gzip.GzipFile(fileobj=fobj)
                if name_lower.endswith(".gz")
                else fobj
            )
            try:
                for line in stream:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue
            finally:
                stream.close()


# ==============================================================================
# METHODS — relation parsing
# ==============================================================================

def parse_relation(rel):
    """Return (src_id, src_type, tgt_id, tgt_type, rel_name).

    Handles both the flat schema (source/sourceType as separate fields) and the
    nested schema (source: {id, type}).
    """
    src = rel.get("source")
    tgt = rel.get("target")

    if isinstance(src, dict):
        src_id, src_type = src.get("id"), src.get("type")
    else:
        src_id, src_type = src, rel.get("sourceType")

    if isinstance(tgt, dict):
        tgt_id, tgt_type = tgt.get("id"), tgt.get("type")
    else:
        tgt_id, tgt_type = tgt, rel.get("targetType")

    rt = rel.get("relType") or rel.get("reltype") or {}
    rel_name = rt.get("name") if isinstance(rt, dict) else None

    return src_id, src_type, tgt_id, tgt_type, rel_name


# ==============================================================================
# METHODS — checkpoint persistence
# ==============================================================================

def load_checkpoint(path, default=None):
    """Load a JSON checkpoint file, returning default if it doesn't exist."""
    if path.exists():
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    return default


def save_checkpoint(path, data):
    """Atomically write a JSON checkpoint file (write-then-rename)."""
    directory = path.parent
    fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
        os.replace(tmp, str(path))
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


# ==============================================================================
# METHODS — time formatting
# ==============================================================================

def format_elapsed(t0):
    """Format elapsed time since t0 (monotonic) as HhMMmSSs."""
    m, s = divmod(int(time.monotonic() - t0), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m{s:02d}s"


# ==============================================================================
# PHASE 0 — read input CSV
# ==============================================================================

def read_input():
    """Read the input CSV and build lookup structures.

    Returns (rows, doi_lookup, pmid_lookup) where:
        rows:        list of dicts with omid, doi_raw, pmid_raw, isbn_raw, doi, pmid
        doi_lookup:  normalized_doi  -> [row_index, ...]
        pmid_lookup: normalized_pmid -> [row_index, ...]

    Rows with no DOI and no PMID are written to MISSING_CSV immediately.
    """
    print("=" * 70)
    print("Phase 0 — reading input CSV")
    print("=" * 70)
    print(f"  Input: {UNIQUE_PIDS_CSV.relative_to(DATA_DIR)}")

    rows = []
    doi_lookup = {}
    pmid_lookup = {}
    missing_rows = []

    with UNIQUE_PIDS_CSV.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            omid = (row.get("omid") or "").strip()
            doi_raw = (row.get("doi") or "").strip()
            pmid_raw = (row.get("pmid") or "").strip()
            isbn_raw = (row.get("isbn") or "").strip()

            ndoi = normalize_doi(doi_raw)
            npmid = normalize_pmid(pmid_raw)

            idx = len(rows)
            rows.append({
                "omid": omid,
                "doi_raw": doi_raw,
                "pmid_raw": pmid_raw,
                "isbn_raw": isbn_raw,
                "doi": ndoi,
                "pmid": npmid,
                "openaire_pub_id": None,
            })

            if ndoi:
                doi_lookup.setdefault(ndoi, []).append(idx)
            if npmid:
                pmid_lookup.setdefault(npmid, []).append(idx)

            if not ndoi and not npmid:
                missing_rows.append(row)

    # Write missing rows immediately
    if missing_rows:
        with MISSING_CSV.open("w", newline="\n", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["omid", "doi", "pmid", "isbn"])
            writer.writeheader()
            writer.writerows(missing_rows)

    searchable = sum(1 for r in rows if r["doi"] or r["pmid"])
    print(f"  {len(rows):,} total rows")
    print(f"  {searchable:,} searchable (have DOI or PMID)")
    print(f"  {len(missing_rows):,} missing (no DOI, no PMID) -> {MISSING_CSV.name}")
    print(f"  {len(doi_lookup):,} unique DOIs, {len(pmid_lookup):,} unique PMIDs")

    return rows, doi_lookup, pmid_lookup


# ==============================================================================
# PHASE 1 — scan publication tars, resolve DOI/PMID -> OpenAIRE publication id
# ==============================================================================

def resolve_publications(rows, doi_lookup, pmid_lookup):
    """Stream all publication tars and match DOIs/PMIDs to OpenAIRE publication ids.

    For each publication record, DOI is checked first, then PMID. The match is
    written into rows[idx]["openaire_pub_id"]. Checkpoints after every tar file.
    """
    print()
    print("=" * 70)
    print("Phase 1 — scanning publication tars")
    print("=" * 70)

    tars = sorted(glob(str(OPENAIRE_DIR / PUBLICATION_TAR_PATTERN)))
    if not tars:
        print(f"  ! no files matching {PUBLICATION_TAR_PATTERN} in {OPENAIRE_DIR}")
        sys.exit(1)
    print(f"  {len(tars)} tar file(s) to process")

    # Resume support
    ckpt = load_checkpoint(CHECKPOINT_PHASE1, {"done_tars": []})
    done_tars = set(ckpt["done_tars"])

    # Rebuild rows from checkpoint if resuming
    if "matched" in ckpt:
        for idx_str, pub_id in ckpt["matched"].items():
            rows[int(idx_str)]["openaire_pub_id"] = pub_id

    total_matched = sum(1 for r in rows if r["openaire_pub_id"])
    if done_tars:
        print(f"  >> resuming: {len(done_tars)} tar(s) already done, "
              f"{total_matched:,} matches so far")

    need_match = sum(
        1 for r in rows
        if (r["doi"] or r["pmid"]) and r["openaire_pub_id"] is None
    )
    print(f"  {need_match:,} rows still need a publication id match")

    if need_match == 0 and done_tars:
        print("  nothing to do — skipping phase 1")
        return

    t0 = time.monotonic()

    for tar_path in tars:
        base = os.path.basename(tar_path)
        if base in done_tars:
            continue

        print(f"  [{format_elapsed(t0)}] processing {base} ...")
        n = hits = 0

        for rec in iter_tar_records(tar_path):
            n += 1
            if n % LOG_EVERY_PUBLICATIONS == 0:
                print(f"    ...{n:,} records | {hits} new matches | "
                      f"total {total_matched:,} | {format_elapsed(t0)}")

            pids_list = rec.get("pids") or []
            rec_id = None

            # Check DOI first, then PMID
            for p in pids_list:
                scheme = (p.get("scheme") or "").lower()
                val = p.get("value")
                if not val:
                    continue

                if scheme == "doi":
                    nd = normalize_doi(val)
                    if nd and nd in doi_lookup:
                        if rec_id is None:
                            rec_id = strip_entity_prefix(rec.get("id"))
                        for idx in doi_lookup[nd]:
                            if rows[idx]["openaire_pub_id"] is None:
                                rows[idx]["openaire_pub_id"] = rec_id
                                hits += 1
                                total_matched += 1

                elif scheme == "pmid":
                    np_ = normalize_pmid(val)
                    if np_ and np_ in pmid_lookup:
                        if rec_id is None:
                            rec_id = strip_entity_prefix(rec.get("id"))
                        for idx in pmid_lookup[np_]:
                            if rows[idx]["openaire_pub_id"] is None:
                                rows[idx]["openaire_pub_id"] = rec_id
                                hits += 1
                                total_matched += 1

        print(f"  [{format_elapsed(t0)}] {base}: {n:,} records, "
              f"{hits} new matches (total: {total_matched:,})")
        done_tars.add(base)

        # Checkpoint: store the matched mapping compactly
        matched_ckpt = {
            str(i): r["openaire_pub_id"]
            for i, r in enumerate(rows) if r["openaire_pub_id"]
        }
        save_checkpoint(CHECKPOINT_PHASE1, {
            "done_tars": sorted(done_tars),
            "matched": matched_ckpt,
        })

    unmatched = sum(
        1 for r in rows
        if (r["doi"] or r["pmid"]) and r["openaire_pub_id"] is None
    )
    print(f"  ✔ Phase 1 done: {total_matched:,} matched, "
          f"{unmatched:,} searchable but unmatched")


# ==============================================================================
# PHASE 2 — scan relation tars, collect pub_id -> [org_id, ...]
# ==============================================================================

def resolve_relations(rows):
    """Stream all relation tars and collect affiliation edges for matched publications.

    Only keeps hasAuthorInstitution / isAuthorInstitutionOf edges where the
    publication id is one we care about. Checkpoints after every tar file.

    Returns pub_to_orgs dict: pub_id -> [org_id, ...].
    """
    print()
    print("=" * 70)
    print("Phase 2 — scanning relation tars")
    print("=" * 70)

    # Build the set of publication ids we care about
    pub_ids = set()
    for r in rows:
        if r["openaire_pub_id"]:
            pub_ids.add(r["openaire_pub_id"])

    if not pub_ids:
        print("  no publication ids to look up — skipping")
        return {}

    print(f"  {len(pub_ids):,} unique publication ids to find affiliations for")

    tars = sorted(glob(str(OPENAIRE_DIR / RELATION_TAR_PATTERN)))
    if not tars:
        print(f"  ! no files matching {RELATION_TAR_PATTERN} in {OPENAIRE_DIR}")
        sys.exit(1)
    print(f"  {len(tars)} tar file(s) to process")

    # Resume support
    ckpt = load_checkpoint(CHECKPOINT_PHASE2, {"done_tars": [], "pub_to_orgs": {}})
    done_tars = set(ckpt["done_tars"])
    pub_to_orgs = ckpt["pub_to_orgs"]

    if done_tars:
        covered = sum(1 for p in pub_ids if pub_to_orgs.get(p))
        print(f"  >> resuming: {len(done_tars)} tar(s) already done, "
              f"{covered:,} pubs with orgs so far")

    t0 = time.monotonic()

    for tar_path in tars:
        base = os.path.basename(tar_path)
        if base in done_tars:
            continue

        print(f"  [{format_elapsed(t0)}] processing {base} ...")
        n = kept = 0

        for rel in iter_tar_records(tar_path):
            n += 1
            if n % LOG_EVERY_RELATIONS == 0:
                print(f"    ...{n:,} relations | {kept} edges kept | "
                      f"{format_elapsed(t0)}")

            src_id, src_type, tgt_id, tgt_type, rel_name = parse_relation(rel)

            if rel_name is None or rel_name.lower() not in AFFILIATION_RELS:
                continue

            # Determine which endpoint is the organization
            if (src_type or "").lower() == "organization":
                org_id, pub_id = src_id, tgt_id
            elif (tgt_type or "").lower() == "organization":
                org_id, pub_id = tgt_id, src_id
            else:
                continue

            pub_id = strip_entity_prefix(pub_id)
            if pub_id not in pub_ids:
                continue

            org_id = strip_entity_prefix(org_id)
            bucket = pub_to_orgs.setdefault(pub_id, [])
            if org_id not in bucket:
                bucket.append(org_id)
                kept += 1

        print(f"  [{format_elapsed(t0)}] {base}: {n:,} relations, "
              f"{kept} new affiliation edges")
        done_tars.add(base)
        save_checkpoint(CHECKPOINT_PHASE2, {
            "done_tars": sorted(done_tars),
            "pub_to_orgs": pub_to_orgs,
        })

    covered = sum(1 for p in pub_ids if pub_to_orgs.get(p))
    print(f"  ✔ Phase 2 done: {covered:,} / {len(pub_ids):,} publications "
          f"have >= 1 organization")

    return pub_to_orgs


# ==============================================================================
# PHASE 3 — scan organization.tar, resolve org ids to records
# ==============================================================================

def resolve_organizations(pub_to_orgs):
    """Resolve OpenAIRE organization ids to authoritative ROR records.

    Two hops: OpenAIRE org id -> ROR id (openaire_ror_map) -> ROR record
    (ror_organizations). Organizations absent from the map had no ROR id, or an
    ambiguous set of them, and are dropped — they never reach the output.
    """
    print()
    print("=" * 70)
    print("Phase 3 — resolving organizations against ROR")
    print("=" * 70)

    wanted = set()
    for org_ids in pub_to_orgs.values():
        wanted.update(org_ids)

    if not wanted:
        print("  no organization ids to resolve — skipping")
        return {}

    print(f"  {len(wanted):,} distinct organization ids to fetch")

    for path in (ROR_ORGANIZATIONS_JSON, OPENAIRE_ROR_MAP_JSON):
        if not path.exists():
            print(f"  ! {path} not found — run match_organizations_countries.py")
            sys.exit(1)

    print(f"  Loading {OPENAIRE_ROR_MAP_JSON.relative_to(DATA_DIR)} ...")
    t0 = time.monotonic()

    with OPENAIRE_ROR_MAP_JSON.open("r", encoding="utf-8") as fh:
        openaire_ror_map = json.load(fh)

    print(f"  Loading {ROR_ORGANIZATIONS_JSON.relative_to(DATA_DIR)} ...")
    with ROR_ORGANIZATIONS_JSON.open("r", encoding="utf-8") as fh:
        ror_organizations = json.load(fh)

    org_lookup = {}
    for oid in wanted:
        ror_id = openaire_ror_map.get(oid)
        if not ror_id:
            continue
        rec = ror_organizations.get(ror_id)
        if not rec:
            continue
        org_lookup[oid] = {
            "legal_name": rec["legal_name"],
            "country_name": rec["country_name"],
            "country_code": rec["country_code"],
            "ror": ror_id,
        }

    dropped = len(wanted) - len(org_lookup)
    distinct = len({rec["ror"] for rec in org_lookup.values()})
    print(f"  ✔ Phase 3 done: {len(org_lookup):,} resolved onto {distinct:,} distinct "
          f"organizations, {dropped:,} dropped (no ROR id or ambiguous) | "
          f"{format_elapsed(t0)}")

    return org_lookup


# ==============================================================================
# OUTPUT — write the merged JSON incrementally
# ==============================================================================

def write_output(rows, pub_to_orgs, org_lookup):
    """Write the final JSON output, flushing periodically to avoid memory issues."""
    print()
    print("=" * 70)
    print("Writing output")
    print("=" * 70)
    print(f"  Output: {OUTPUT_JSON.relative_to(DATA_DIR)}")

    t0 = time.monotonic()
    written = 0
    first = True

    with OUTPUT_JSON.open("w", encoding="utf-8") as fh:
        fh.write("{\n")

        for r in rows:
            omid = r["omid"]
            if not omid:
                continue

            pub_id = r["openaire_pub_id"] or ""

            # Collect organizations, deduplicating on ROR id — several OpenAIRE
            # org ids can resolve to the same organization
            org_ids = pub_to_orgs.get(pub_id, []) if pub_id else []
            orgs_by_ror = {}
            for oid in org_ids:
                orec = org_lookup.get(oid)
                if orec:
                    orgs_by_ror[orec["ror"]] = orec
            orgs = list(orgs_by_ror.values())

            # Strip scheme prefixes for output pids
            doi_out = r["doi_raw"]
            if doi_out.lower().startswith("doi:"):
                doi_out = doi_out[4:]
            pmid_out = r["pmid_raw"]
            if pmid_out.lower().startswith("pmid:"):
                pmid_out = pmid_out[5:]
            isbn_out = r["isbn_raw"]
            if isbn_out.lower().startswith("isbn:"):
                isbn_out = isbn_out[5:]

            entry = {
                "pids": {
                    "doi": doi_out,
                    "pmid": pmid_out,
                    "isbn": isbn_out,
                    "openaire": pub_id,
                },
                "organizations": orgs,
            }

            line = json.dumps(entry, ensure_ascii=False)
            key = json.dumps(omid, ensure_ascii=False)

            if not first:
                fh.write(",\n")
            fh.write(f"  {key}: {line}")
            first = False
            written += 1

            if written % FLUSH_EVERY == 0:
                fh.flush()
                print(f"  {written:,} entries written | {format_elapsed(t0)}")

        fh.write("\n}\n")

    print(f"  ✔ {written:,} entries written -> {OUTPUT_JSON.name} | "
          f"{format_elapsed(t0)}")

    return written


# ==============================================================================
# RUNTIME
# ==============================================================================

# Skip if output already exists
if OUTPUT_JSON.exists():
    print(f"! output already exists, skipping: {OUTPUT_JSON.relative_to(DATA_DIR)}")
    sys.exit(0)

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Start monotonic timer
started_at = time.monotonic()

# Phase 0 — read input CSV
rows, doi_lookup, pmid_lookup = read_input()

# Phase 1 — scan publication tars, resolve DOI/PMID -> OpenAIRE publication id
resolve_publications(rows, doi_lookup, pmid_lookup)

# Phase 2 — scan relation tars, collect pub_id -> [org_id, ...]
pub_to_orgs = resolve_relations(rows)

# Phase 3 — scan organization.tar, resolve org ids to records
org_lookup = resolve_organizations(pub_to_orgs)

# Write the merged JSON output
entries_written = write_output(rows, pub_to_orgs, org_lookup)

# Clean up checkpoint files on success
for checkpoint_file in (CHECKPOINT_PHASE1, CHECKPOINT_PHASE2):
    if checkpoint_file.exists():
        checkpoint_file.unlink()

# Record end time and calculate elapsed time
ended_at = datetime.now(timezone.utc)
elapsed_seconds = round(time.monotonic() - started_at, 2)
output_size_bytes = OUTPUT_JSON.stat().st_size if OUTPUT_JSON.exists() else 0
missing_size_bytes = MISSING_CSV.stat().st_size if MISSING_CSV.exists() else 0

# Compile metadata about the processing run
metadata = {
    "elapsed_seconds": elapsed_seconds,
    "ended_at": ended_at.isoformat(),
    "rows_read": len(rows),
    "rows_searchable": sum(1 for r in rows if r["doi"] or r["pmid"]),
    "rows_matched_publication": sum(1 for r in rows if r["openaire_pub_id"]),
    "rows_with_organizations": sum(
        1 for r in rows
        if r["openaire_pub_id"] and pub_to_orgs.get(r["openaire_pub_id"])
    ),
    "rows_missing_no_pid": sum(1 for r in rows if not r["doi"] and not r["pmid"]),
    "openaire_org_ids_resolved": len(org_lookup),
    "unique_organizations_resolved": len({rec["ror"] for rec in org_lookup.values()}),
    "entries_written": entries_written,
    "flush_every": FLUSH_EVERY,
    "output_json_size_bytes": output_size_bytes,
    "output_json_size_mb": round(output_size_bytes / 1024 / 1024, 2),
    "missing_csv_size_bytes": missing_size_bytes,
    "missing_csv_size_mb": round(missing_size_bytes / 1024 / 1024, 2),
}

# Write metadata to JSON file
with OUTPUT_METADATA.open("w", encoding="utf-8") as f:
    json.dump(metadata, f, indent=2)

print(f"\n✔ all done in {elapsed_seconds}s")
print(f"  Output JSON: {OUTPUT_JSON.relative_to(DATA_DIR)}")
print(f"  Missing CSV: {MISSING_CSV.relative_to(DATA_DIR)}")
print(f"  Metadata: {OUTPUT_METADATA.relative_to(DATA_DIR)}")
