import csv
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = ROOT_DIR / "data" / "iris_oc_pids"
OUTPUT_FILE = ROOT_DIR / "data" / "unique_pids.csv"
WRITE_EVERY_ROWS = 500_000

PID_TYPES = ["omid", "doi", "pmid", "isbn"]

pid_groups = []
pid_to_group_index = {}
rows_processed = 0


def write_output():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=PID_TYPES)
        writer.writeheader()

        for group in sorted(
            pid_groups,
            key=lambda item: tuple(item[pid_type] for pid_type in PID_TYPES),
        ):
            writer.writerow(group)


for csv_path in sorted(INPUT_DIR.glob("*/iris_oc_pids.csv")):
    print(f"Reading {csv_path}")

    with csv_path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)

        for row in reader:
            rows_processed += 1

            records = [
                {
                    "omid": row.get("citing_omid", "").strip(),
                    "doi": row.get("citing_doi", "").strip(),
                    "pmid": row.get("citing_pmid", "").strip(),
                    "isbn": row.get("citing_isbn", "").strip(),
                },
                {
                    "omid": row.get("cited_omid", "").strip(),
                    "doi": row.get("cited_doi", "").strip(),
                    "pmid": row.get("cited_pmid", "").strip(),
                    "isbn": row.get("cited_isbn", "").strip(),
                },
            ]

            for record in records:
                present_pids = [
                    (pid_type, record[pid_type])
                    for pid_type in PID_TYPES
                    if record[pid_type]
                ]

                if not present_pids:
                    continue

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

            if rows_processed % WRITE_EVERY_ROWS == 0:
                write_output()
                print(
                    f"Wrote checkpoint after {rows_processed:,} rows "
                    f"with {len(pid_groups):,} unique PID groups"
                )


write_output()

print(f"Wrote {len(pid_groups):,} unique PID groups to {OUTPUT_FILE}")
