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

INSTITUTION_LABELS = {
    "UNIBO": "University of Bologna",
    "UNIMI": "University of Milan",
    "UNIPD": "University of Padua",
    "UNITO": "University of Turin",
    "UPO":   "University of Eastern Piedmont",
    "SNS":   "Scuola Normale Superiore",
}

# Canonical country names for ambiguous cases (following common English usage)
COUNTRY_NAMES = {
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
    "VI": "U.S. Virgin Islands",
    "VN": "Vietnam",
    "XK": "Kosovo",
}

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

    # Apply canonical name mapping (only affects ambiguous cases)
    df["country_name"] = df["country_code"].map(COUNTRY_NAMES).fillna(df["country_name"])

    # Aggregate: sum counts for rows that now share the same (code, name) after mapping
    df = (df.groupby(["country_code", "country_name"], as_index=False)["count"]
            .sum())
    return df


def load_country_data(institution: str, direction: str, base_path: Path = None) -> pd.DataFrame:
    """Load a citation-counts CSV for one institution and direction.
    
    Parameters
    ----------
    institution : str  e.g. 'UNIBO'
    direction   : str  'incoming' or 'outgoing'
    base_path   : Path, optional
        Custom path to the directory containing institution folders.
    
    Returns
    -------
    pd.DataFrame with columns: country_code, country_name, count, institution, direction
    """
    if base_path is None:
        base_path = BASE_PATH
    path = base_path / institution / f"citation_counts_countries_{direction}.csv"
    df = pd.read_csv(path)

    # ── Cleaning & Normalization ──
    df = normalize_countries(df)

    # ── Add metadata ──
    df["institution"] = institution
    df["direction"]   = direction

    return df


def load_institution(institution: str, exclude_self: bool = True, base_path: Path = None) -> pd.DataFrame:
    """Load and merge incoming + outgoing for one institution.
    
    Parameters
    ----------
    institution : str  e.g. 'UNIBO'
    exclude_self : bool
        If True, drop Italy (IT) to focus on *international* relationships.
    base_path   : Path, optional
        Custom path to the directory containing institution folders.
        
    Returns
    -------
    pd.DataFrame
    """
    incoming = load_country_data(institution, "incoming", base_path=base_path)
    outgoing = load_country_data(institution, "outgoing", base_path=base_path)
    
    df = pd.concat([incoming, outgoing], ignore_index=True)
    
    if exclude_self:
        df = df[df["country_code"] != "IT"]
    
    return df


def load_all(exclude_self: bool = True, base_path: Path = None) -> pd.DataFrame:
    """Load data for ALL institutions into one long-format DataFrame."""
    frames = [load_institution(inst, exclude_self=exclude_self, base_path=base_path)
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