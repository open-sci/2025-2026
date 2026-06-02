from pathlib import Path
import pandas as pd

from .data_utils import (
    INSTITUTIONS,
    load_country_data, 
    load_org_data
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

def validate_dataset(df, duplicate_subset):
    """
    Generic validation for cleaned datasets.
    """

    print("Checking dataset...")

    dupes = df[
        df.duplicated(
            subset=duplicate_subset,
            keep=False
        )
    ]

    if dupes.empty:
        print("✓ No duplicates found")
    else:
        print("✗ Duplicates found")
        print(dupes)

    if df.isnull().any().any():
        print("✗ Missing values found")
    else:
        print("✓ No missing values")


def export_cleaned_csvs(
    loader_function,
    filename_prefix: str,
    output_dir: Path,
    institutions,
):
    """
    Export cleaned datasets using the provided loader.

    Parameters
    ----------
    loader_function : callable
        e.g. load_country_data or load_org_data

    filename_prefix : str
        'countries' or 'organizations'

    output_dir : Path

    institutions : iterable
        INSTITUTIONS list/dict
    """

    output_dir = Path(output_dir)

    for inst in institutions:
        for direction in ["incoming", "outgoing"]:
            try:
                df = loader_function(inst, direction)
                out_path = (
                    output_dir
                    / inst
                    / f"citation_counts_{filename_prefix}_{direction}_clean.csv"
                )
                out_path.parent.mkdir(
                    parents=True,
                    exist_ok=True
                )
                df.to_csv(out_path, index=False)
                print(f"Saved: {out_path}")
            except FileNotFoundError:
                print(
                    f"Skipped {inst} {direction}"
                )