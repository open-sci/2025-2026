from pathlib import Path
import pandas as pd
import numpy as np
import pycountry

# ==================================================
# CONSTANTS
# ==================================================

# ── Paths ──
BASE_PATH = Path(__file__).resolve().parent.parent / "data" / "citation_counts"

INSTITUTIONS = ["UNIBO", "UNIMI", "UNIPD", "UNITO", "UPO", "SNS"]

EXCLUDE_ITALIAN_PARTNERS = False

INSTITUTION_LABELS = {
    "UNIBO": "University of Bologna",
    "UNIMI": "University of Milan",
    "UNIPD": "University of Padua",
    "UNITO": "University of Turin",
    "UPO":   "University of Eastern Piedmont",
    "SNS":   "Scuola Normale Superiore",
}

# Legal name as it appears in the CSV — used to exclude only the focal institution
INSTITUTION_SELF_NAMES = {
    "UNIBO": "University of Bologna",
    "UNIMI": "University of Milan",
    "UNIPD": "University of Padua",
    "UNITO": "University of Turin",
    "UPO":   "University of Eastern Piedmont",
    "SNS":   "Scuola Normale Superiore",
}

# Canonical country names for ambiguous cases (following common English usage)
COUNTRY_NAMES = {
    # --- territory-parent merges (the only codes with duplicate issues) ---
    "DK": "Denmark",
    "FI": "Finland",
    "FR": "France",
    "GB": "United Kingdom",
    "NO": "Norway",
    "US": "United States",

    # --- existing ambiguous name overrides ---
    "BN": "Brunei",
    "CD": "DR Congo",
    "CG": "Congo Republic",
    "CI": "Ivory Coast",
    "CN": "China",
    "CV": "Cabo Verde",
    "CZ": "Czechia",
    "IR": "Iran",
    "KP": "North Korea",
    "KR": "South Korea",
    "LA": "Laos",
    "LY": "Libya",
    "MD": "Moldova",
    "MK": "North Macedonia",
    "NL": "The Netherlands",
    "PS": "Palestine",
    "RU": "Russia",
    "SY": "Syria",
    "SZ": "Eswatini",
    "TR": "Turkey",         
    "TZ": "Tanzania",
    "VN": "Vietnam",
    "XK": "Kosovo",
}

# Maps territory ISO-2 codes to their parent country's ISO-2 code
TERRITORY_TO_PARENT = {
    # United States territories
    "AS": "US",  # American Samoa
    "GU": "US",  # Guam
    "MP": "US",  # Northern Mariana Islands
    "PR": "US",  # Puerto Rico
    "UM": "US",  # United States Minor Outlying Islands
    "VI": "US",  # U.S. Virgin Islands  ← remove from COUNTRY_NAMES too

    # France territories
    "GF": "FR",  # French Guiana
    "GP": "FR",  # Guadeloupe
    "MQ": "FR",  # Martinique
    "RE": "FR",  # Réunion
    "YT": "FR",  # Mayotte
    "PF": "FR",  # French Polynesia
    "NC": "FR",  # New Caledonia

    # Netherlands territories
    "AW": "NL",  # Aruba
    "CW": "NL",  # Curaçao
    "BQ": "NL",  # Bonaire, Sint Eustatius, and Saba
    "SX": "NL",  # Sint Maarten

    # United Kingdom territories
    "GI": "GB",  # Gibraltar
    "IM": "GB",  # Isle of Man
    "JE": "GB",  # Jersey
    "FK": "GB",  # Falkland Islands
    "BM": "GB",  # Bermuda
    "KY": "GB",  # Cayman Islands
    "TC": "GB",  # Turks and Caicos Islands
    "VG": "GB",  # British Virgin Islands
    "MS": "GB",  # Montserrat

    # China territories
    "MO": "CN",  # Macao
    "HK": "CN",  # Hong Kong

    # Finland / Denmark / Norway
    "AX": "FI",  # Åland
    "FO": "DK",  # Faroe Islands
    "GL": "DK",  # Greenland
    "SJ": "NO",  # Svalbard and Jan Mayen
}

ANNUALIZED_BASE_PATH = Path(__file__).resolve().parent.parent / "data" / "citation_counts_annualized"
AGGREGATE_VISUALIZATIONS_PATH = Path(__file__).resolve().parent.parent / "data" / "visualizations"
VISUALIZATIONS_PATH = Path(__file__).resolve().parent.parent / "data" / "visualizations" / "citation_counts_annualized"

YEAR_BLOCKS = ['2001-2005', '2006-2010', '2011-2015', '2016-2020', '2021-2025']

def normalize_countries(df):
    """Normalize country names and codes, handling duplicates and canonical names.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with columns 'country_code', 'country_name', 'count'.
        
    Returns
    -------
    pd.DataFrame
        Cleaned, normalized, and aggregated DataFrame.
    """
    # Drop rows with missing country_code or country_name 
    df = df.dropna(subset=["country_code", "country_name"])
    df = df[df["country_code"].str.strip() != ""]

    # Normalize country_code formatting
    df["country_code"] = df["country_code"].str.strip().str.upper()

    # Remap territories to parent country codes FIRST
    df["country_code"] = df["country_code"].map(TERRITORY_TO_PARENT).fillna(df["country_code"])

    # Apply canonical name mapping (only affects ambiguous cases)
    df["country_name"] = df["country_code"].map(COUNTRY_NAMES).fillna(df["country_name"])

    # Aggregate: sum counts for rows that now share the same (code, name) after mapping
    df = (df.groupby(["country_code", "country_name"], as_index=False)["count"]
            .sum())
    return df


def load_country_data(institution: str, direction: str, base_path: Path = None, suffix: str = "") -> pd.DataFrame:
    """Load a citation-counts CSV for one institution and direction.
    
    Parameters
    ----------
    institution : str  e.g. 'UNIBO'
    direction   : str  'incoming' or 'outgoing'
    base_path   : Path, optional
        Custom path to the directory containing institution folders.
    suffix      : str, optional
        Suffix to append to the file name before the extension.
    
    Returns
    -------
    pd.DataFrame with columns: country_code, country_name, count, institution, direction
    """
    if base_path is None:
        base_path = BASE_PATH
    path = base_path / institution / f"citation_counts_countries_{direction}{suffix}.csv"
    df = pd.read_csv(path)

    # ── Cleaning & Normalization ──
    df = normalize_countries(df)

    # ── Add metadata ──
    df["institution"] = institution
    df["direction"]   = direction

    return df


def load_institution(institution: str, exclude_self: bool = True, base_path: Path = None, suffix: str = "") -> pd.DataFrame:
    """Load and merge incoming + outgoing for one institution.
    
    Parameters
    ----------
    institution : str  e.g. 'UNIBO'
    exclude_self : bool
        If True, drop Italy (IT) to focus on *international* relationships.
    base_path   : Path, optional
        Custom path to the directory containing institution folders.
    suffix      : str, optional
        Suffix to append to the file name before the extension.
        
    Returns
    -------
    pd.DataFrame
    """
    incoming = load_country_data(institution, "incoming", base_path=base_path, suffix=suffix)
    outgoing = load_country_data(institution, "outgoing", base_path=base_path, suffix=suffix)
    
    df = pd.concat([incoming, outgoing], ignore_index=True)
    
    if exclude_self:
        df = df[df["country_code"] != "IT"]
    
    return df


def load_all(exclude_self: bool = True, base_path: Path = None, suffix: str = "") -> pd.DataFrame:
    """Load data for ALL institutions into one long-format DataFrame."""
    frames = [load_institution(inst, exclude_self=exclude_self, base_path=base_path, suffix=suffix)
              for inst in INSTITUTIONS]
    return pd.concat(frames, ignore_index=True)


def pivot_directions(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot a long-format single-institution DataFrame to wide format.
    
    Returns a DataFrame with columns:
      country_code, country_name, incoming_count, outgoing_count,
      total, difference (outgoing − incoming), ratio (out/in)
    """
    wide = df.pivot_table(
        index=["country_code", "country_name"],
        columns="direction",
        values="count",
        aggfunc="sum"
    ).reset_index()
    wide.columns.name = None
    wide = wide.rename(columns={"incoming": "incoming_count",
                                 "outgoing": "outgoing_count"})
    wide = wide.fillna(0)
    wide["total"]      = wide["incoming_count"] + wide["outgoing_count"]
    wide["difference"] = wide["outgoing_count"] - wide["incoming_count"]
    # ratio: log-scale-friendly asymmetry: log2(out/in)
    # guard against zeros
    wide["log2_ratio"] = np.log2(
        (wide["outgoing_count"] + 1) / (wide["incoming_count"] + 1)
    )
    return wide.sort_values("total", ascending=False)


def to_iso3(code2):
    """Convert ISO 3166-1 alpha-2 to alpha-3. Returns None if not found."""
    try:
        return pycountry.countries.get(alpha_2=code2).alpha_3
    except AttributeError:
        return None
    

def normalize_organizations(df: pd.DataFrame, institution: str) -> pd.DataFrame:
    """Normalize and clean organization-level data."""
    # 1. Drop rows missing key identifiers
    df = df.dropna(subset=["country_code", "country_name", "legal_name"])
    df = df[df["country_code"].str.strip() != ""]

    # 2. Normalise country_code
    df["country_code"] = df["country_code"].str.strip().str.upper()

    # 3. Remap territories to parent country codes + apply canonical country name mapping
    df["country_code"] = df["country_code"].map(TERRITORY_TO_PARENT).fillna(df["country_code"])
    df["country_name"] = df["country_code"].map(COUNTRY_NAMES).fillna(df["country_name"])

    # 4. Aggregate counts for any (ror, country_code) duplicates after mapping
    group_cols = ["ror", "legal_name", "country_code", "country_name"] if "ror" in df.columns else ["legal_name", "country_code", "country_name"]
    df = df.groupby(group_cols, as_index=False)["count"].sum()

    # 5. Remove the focal institution's own self-citation row
    self_name = INSTITUTION_SELF_NAMES[institution]
    df = df[df["legal_name"] != self_name].copy()

    # 6. Optionally exclude all other Italian partner institutions
    if EXCLUDE_ITALIAN_PARTNERS:
        df = df[df["country_code"] != "IT"].copy()

    return df


def load_org_data(institution: str, direction: str, base_path: Path = None, suffix: str = "") -> pd.DataFrame:
    """Load and clean the org-level CSV for one institution and direction.

    Cleaning steps (mirrors load_country_data in the country-level notebook):
      1. Drop rows with missing country_code, country_name, or ror.
      2. Normalise country_code formatting (strip whitespace, uppercase).
      3. Apply COUNTRY_NAMES canonical mapping (same dict as country notebook).
      4. Aggregate counts for rows sharing the same (ror, country_code) after mapping.
      5. Remove the focal institution's own self-citation row.

    Returns
    -------
    pd.DataFrame with columns: ror, legal_name, country_code, country_name,
                                count, institution, direction
    """
    if base_path is None:
        base_path = BASE_PATH
    path = base_path / institution / f"citation_counts_organizations_{direction}{suffix}.csv"
    df   = pd.read_csv(path)

    df = normalize_organizations(df, institution)

    # Add metadata
    df["institution"] = institution
    df["direction"]   = direction

    return df


def load_all_available(base_path: Path = None, suffix: str = "") -> dict:
    """Load all institutions for which CSV files are present.
    Returns: {inst_key: (incoming_df, outgoing_df)}
    """
    datasets = {}
    for inst in INSTITUTIONS:
        try:
            inb = load_org_data(inst, "incoming", base_path=base_path, suffix=suffix)
            out = load_org_data(inst, "outgoing", base_path=base_path, suffix=suffix)
            datasets[inst] = (inb, out)
            print(f"✓ {inst}: incoming {len(inb):,} orgs · outgoing {len(out):,} orgs")
        except FileNotFoundError:
            print(f"○ {inst}: data not yet available — skipped")
    return datasets

def load_temporal_dataset(institution: str, direction: str, year_block: str, dataset_type: str, base_path: Path = ANNUALIZED_BASE_PATH) -> pd.DataFrame:
    """Loads raw temporal data securely (dataset_type must be 'organizations' or 'countries')."""
    file_path = base_path / institution / year_block / f"citation_counts_{dataset_type}_{direction}.csv"
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
        
    df = pd.read_csv(file_path)

    # ── Cleaning & Normalization ──
    if dataset_type == "countries":
        df = normalize_countries(df)
    elif dataset_type == "organizations":
        df = normalize_organizations(df, institution)

    # ── Add metadata ──
    df["italian_institution"] = institution
    df["year_block"] = year_block
    df["direction"] = direction
    return df


def load_all_temporal(dataset_type: str = "organizations", base_path: Path = None) -> pd.DataFrame:
    if base_path is None:
        base_path = ANNUALIZED_BASE_PATH
    frames = []
    for inst in INSTITUTIONS:
        for y_block in YEAR_BLOCKS:
            for direction in ["incoming", "outgoing"]:
                try:
                    df = load_temporal_dataset(inst, direction, y_block, dataset_type, base_path)
                    frames.append(df)
                except FileNotFoundError:
                    pass

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)