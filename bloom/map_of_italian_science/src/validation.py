from pathlib import Path
import pandas as pd

from .data_utils import (
    INSTITUTIONS,
    load_country_data
)

def discover_country_name_variants(base_path):
    """
    Scan all country CSVs and identify country codes
    associated with multiple country names.

    Used once to build COUNTRY_NAMES mapping dictionary.
    """

    for inst in INSTITUTIONS:
        for direction in ["incoming", "outgoing"]:
            raw = pd.read_csv(
                Path(base_path)
                / inst
                / f"citation_counts_countries_{direction}.csv"
            )
            raw = raw.dropna(subset=["country_code"])
            dupes = (
                raw.groupby("country_code")["country_name"]
                .nunique()
            )
            dupes = dupes[dupes > 1].index

            if len(dupes) > 0:
                print(f"\n{inst} {direction}")
                print(
                    raw[
                        raw["country_code"].isin(dupes)
                    ][
                        ["country_code", "country_name"]
                    ]
                    .drop_duplicates()
                    .sort_values("country_code")
                )

def validate_dataset(df):
    print("Checking dataset...")
    dupes = df[
        df.duplicated(
            subset=["country_code"],
            keep=False
        )
    ]

    if dupes.empty:
        print("✓ No duplicate country codes")
    else:
        print("✗ Duplicate country codes found")
        print(dupes)

    if (
        df[
            ["country_code",
             "country_name",
             "count"]
        ]
        .isnull()
        .any()
        .any()
    ):
        print("✗ Missing values found")
    else:
        print("✓ No missing values")

def validate_all(base_path):
    found_issues = False
    for inst in INSTITUTIONS:
        for direction in ["incoming", "outgoing"]:
            df = load_country_data(
                inst,
                direction,
                base_path
            )
            dupes = df[
                df.duplicated(
                    subset=["country_code"],
                    keep=False
                )
            ]

            if not dupes.empty:
                print(
                    f"✗ {inst} {direction}: duplicates found"
                )
                print(dupes)
                found_issues = True

    if not found_issues:
        print(
            "✓ No duplicate country codes found in any dataset"
        )

def export_cleaned_csvs(base_path, output_path):
    for inst in INSTITUTIONS:
        for direction in ["incoming", "outgoing"]:
            df = load_country_data(
                inst,
                direction,
                base_path
            )

            out_file = (
                Path(output_path)
                / inst
                / f"citation_counts_countries_{direction}_clean.csv"
            )

            out_file.parent.mkdir(
                parents=True,
                exist_ok=True
            )

            df.to_csv(
                out_file,
                index=False
            )

            print(f"Saved: {out_file}")