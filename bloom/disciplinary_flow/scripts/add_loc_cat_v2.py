"""
DESCRIPTION
-----------
This script enriches a citation-flow CSV file with Library of Congress (LOC)
main classification labels and URIs for both the citing and cited publications.

Four new columns are added to the output:
    - citing_loc_label  : LOC main class label(s) for the citing publication
    - citing_loc_uri    : LOC main class URI(s) for the citing publication
    - cited_loc_label   : LOC main class label(s) for the cited publication
    - cited_loc_uri     : LOC main class URI(s) for the cited publication

When multiple LOC classes are matched, values are joined with ' | '.

INPUT FILES
-----------
    1. CSV file (CSV_PATH)
       Expected columns used by this script:
           citing_scimago_area, citing_scimago_category
           citing_doaj_lcc, citing_doaj_subject
           cited_scimago_area,  cited_scimago_category
           cited_doaj_lcc,  cited_doaj_subject

    2. JSON file (JSON_PATH)
       A list of LOC classification entries, each with the structure:
           {
               "loc_id": "http://id.loc.gov/authorities/classification/QH1-QH278.5",
               "label": "Natural history (General)",
               "alt_labels": [...],
               "scopus_alignments": [
                   {
                       "area_labels": ["Biology"],
                       "category_labels": ["Ecology", ...]
                   }
               ]
           }

ALIGNMENT LOGIC
---------------
For each publication (citing / cited), the script tries to resolve a LOC main
class using the following priority:

    1. Scimago  (scimago_area and scimago_category columns)
       - Scimago classifies journals at two hierarchical levels: area
         (broad, e.g. "Life Sciences") and category (specific, e.g.
         "Ecology"). Both are looked up independently and their LOC
         matches combined, since the JSON may index either level or both.
       - Category labels are cleaned of quartile suffixes like "(Q1)".
       - Multiple values separated by ";" are all processed.
       - The first letter of the matched loc_id path is extracted
         (e.g. "QH1-QH278.5" -> "Q") and mapped to the LOC main class.
       - For area_labels, only skos:closeMatch and skos:exactMatch
         alignments are used. skos:narrowMatch and skos:broadMatch are
         excluded because broad Scimago area labels (e.g. "Engineering")
         can appear in narrowMatch entries for highly specific LOC classes
         (e.g. "Biomedical Engineering" -> Medicine (General)), leading to
         incorrect assignments. Category labels are not filtered by match
         type as they are already specific enough to be unambiguous.

    2. DOAJ  (doaj_subject column, fallback when Scimago yields no result)
       - The general category is extracted from doaj_subject by taking
         the part before the first ":" (e.g. "Science: Physics" -> "Science").
       - Multiple subjects separated by "|" are all processed.
       - The extracted string is matched against LOC main class labels.

LIMITATIONS
-----------
    - Publications with no Scimago and no DOAJ data will produce NaN values
      in the new columns. This is expected and reflects missing upstream data.

    - Some Scimago areas (e.g. "Business, Management and Accounting",
      "Computer Science", "Multidisciplinary") have no corresponding entry
      in the JSON alignment file, resulting in unresolved LOC classes.
      No custom mappings were added: the alignment intentionally relies on
      a JSON produced by a previous researcher, and matching is kept strictly
      exact to avoid arbitrary classification decisions. Broader strategies
      such as 'contains' or 'startswith' risk misclassifying ambiguous labels
      (e.g. "Science" matching "Computer Science" or "Political Science").

    - "Multidisciplinary" cannot be meaningfully mapped to a single LOC class by design, but it is still added as a separate category (v2).
    
    -When a subject is present in the Scimago fields but fails to match any LOC class in the JSON, the row is labelled "Others" instead of being left unresolved. This allows us to distinguish between rows with no data at all (discarded) and rows with data that simply doesn't align to LOC (Others) (v2).

    - When a Scimago area or category is aligned to multiple LOC classes in
      the JSON (e.g. "Biochemistry, Genetics and Molecular Biology" maps to
      both H and Q), all matched classes are returned joined by ' | '.

    - The JSON alignment uses four SKOS match properties: skos:closeMatch,
      skos:exactMatch, skos:narrowMatch, and skos:broadMatch. When resolving
      via area_labels, only closeMatch and exactMatch are trusted. Using
      narrowMatch or broadMatch at the area level causes over-broad
      assignments: for example, the area label "Engineering" appears in a
      narrowMatch entry linking Biomedical Engineering to Medicine (General),
      which would incorrectly assign MEDICINE to any Engineering journal.
      This filter is applied only to area_labels; category_labels are used
      regardless of match type since they should be specific enough to be safe.

    - Rows where at least one side (citing or cited) has no LOC label resolved
      are excluded from output_cat.csv and saved separately in miss_loc.csv
      for future inspection.

OUTPUT
------
    output_cat.csv : rows where BOTH citing and cited have a LOC label resolved.
    miss_loc.csv   : rows where at least one side (citing or cited) has no LOC
                     label. These are saved for inspection rather than discarded.
    Both files are saved in the same directory from which the script is run.
"""

import json
import os
import re

import pandas as pd


CSV_PATH  = "SNS/disciplinary_map_matched.csv"
JSON_PATH = "/Users/regina/Documents/UNIBO/OpenScience/2025-2026/bloom/disciplinary_flow/categories/merged_loc_scopus.json"


LOC_MAIN_CLASSES = {
    "A": {"label": "GENERAL WORKS",
          "uri": "http://id.loc.gov/authorities/classification/A"},
    "B": {"label": "PHILOSOPHY. PSYCHOLOGY. RELIGION",
          "uri": "http://id.loc.gov/authorities/classification/B"},
    "C": {"label": "AUXILIARY SCIENCES OF HISTORY",
          "uri": "http://id.loc.gov/authorities/classification/C"},
    "D": {"label": "WORLD HISTORY AND HISTORY OF EUROPE, ASIA, AFRICA, AUSTRALIA, NEW ZEALAND, ETC.",
          "uri": "http://id.loc.gov/authorities/classification/D"},
    "E": {"label": "HISTORY OF THE AMERICAS",
          "uri": "http://id.loc.gov/authorities/classification/E"},
    "F": {"label": "HISTORY OF THE AMERICAS",
          "uri": "http://id.loc.gov/authorities/classification/F"},
    "G": {"label": "GEOGRAPHY. ANTHROPOLOGY. RECREATION",
          "uri": "http://id.loc.gov/authorities/classification/G"},
    "H": {"label": "SOCIAL SCIENCES",
          "uri": "http://id.loc.gov/authorities/classification/H"},
    "J": {"label": "POLITICAL SCIENCE",
          "uri": "http://id.loc.gov/authorities/classification/J"},
    "K": {"label": "LAW",
          "uri": "http://id.loc.gov/authorities/classification/K"},
    "L": {"label": "EDUCATION",
          "uri": "http://id.loc.gov/authorities/classification/L"},
    "M": {"label": "MUSIC AND BOOKS ON MUSIC",
          "uri": "http://id.loc.gov/authorities/classification/M"},
    "N": {"label": "FINE ARTS",
          "uri": "http://id.loc.gov/authorities/classification/N"},
    "P": {"label": "LANGUAGE AND LITERATURE",
          "uri": "http://id.loc.gov/authorities/classification/P"},
    "Q": {"label": "SCIENCE",
          "uri": "http://id.loc.gov/authorities/classification/Q"},
    "R": {"label": "MEDICINE",
          "uri": "http://id.loc.gov/authorities/classification/R"},
    "S": {"label": "AGRICULTURE",
          "uri": "http://id.loc.gov/authorities/classification/S"},
    "T": {"label": "TECHNOLOGY",
          "uri": "http://id.loc.gov/authorities/classification/T"},
    "U": {"label": "MILITARY SCIENCE",
          "uri": "http://id.loc.gov/authorities/classification/U"},
    "V": {"label": "NAVAL SCIENCE",
          "uri": "http://id.loc.gov/authorities/classification/V"},
    "Z": {"label": "BIBLIOGRAPHY. LIBRARY SCIENCE. INFORMATION RESOURCES",
          "uri": "http://id.loc.gov/authorities/classification/Z"},
}
# reference: https://www.loc.gov/catdir/cpso/lcco/

# Reverse lookup: LOC label (uppercase) -> single letter key
LOC_LABEL_TO_LETTER = {v["label"].upper(): k for k, v in LOC_MAIN_CLASSES.items()}


def load_scopus_index(json_path: str) -> tuple[dict, dict]:
    """
    Load the LOC-Scopus alignment JSON and build two lookup dictionaries:
        area_label_to_letters : lowercased area label    -> set of LOC letters
        cat_label_to_letters  : lowercased category label -> set of LOC letters

    The LOC main letter is extracted from the loc_id path, e.g.:
        "http://id.loc.gov/authorities/classification/BF1-BF990" -> "B"

    For area_labels, only skos:closeMatch and skos:exactMatch alignments are
    indexed. skos:narrowMatch and skos:broadMatch are excluded to prevent
    over-broad assignments (see LIMITATIONS in the module docstring).
    Category labels are indexed regardless of match type.
    """
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    area_label_to_letters: dict[str, set] = {}
    cat_label_to_letters:  dict[str, set] = {}

    # Only these match types are trusted for area-level lookup
    TRUSTED_MATCH_TYPES = {"closeMatch", "exactMatch"}

    for item in data:
        loc_id = item.get("loc_id", "")
        match = re.search(r"/classification/([A-Z]+)", loc_id)
        if not match:
            continue
        letter = match.group(1)[0]

        for alignment in item.get("scopus_alignments", []):
            match_type = alignment.get("type", "")
            if match_type in TRUSTED_MATCH_TYPES:
                for area_label in alignment.get("area_labels", []):
                    key = area_label.strip().lower()
                    area_label_to_letters.setdefault(key, set()).add(letter)
            for cat_label in alignment.get("category_labels", []):
                key = cat_label.strip().lower()
                cat_label_to_letters.setdefault(key, set()).add(letter)

    return area_label_to_letters, cat_label_to_letters


def letters_to_results(letters: set) -> tuple[list, list]:
    """Convert a set of LOC letters to sorted parallel lists of labels and URIs."""
    labels, uris = [], []
    for letter in sorted(letters):
        entry = LOC_MAIN_CLASSES.get(letter.upper())
        if entry:
            labels.append(entry["label"])
            uris.append(entry["uri"])
    return labels, uris


def format_result(items: list) -> str | None:
    """Deduplicate and join a list of values with ' | ', or return None if empty."""
    unique = list(dict.fromkeys(items))
    return " | ".join(unique) if unique else None


def resolve_doaj(doaj_subject_val) -> tuple[list, list]:
    """
    Resolve LOC classes from a doaj_subject value.

    The doaj_subject field uses the format:
        "General category: subcategory: ..."
    with multiple subjects separated by "|", e.g.:
        "Science: Physics | Medicine: Geriatrics"

    Only the part before the first ":" is used for each subject
    and matched against LOC main class labels.
    """
    if pd.isna(doaj_subject_val):
        return [], []

    letters = set()
    for part in str(doaj_subject_val).split("|"):
        general_category = part.strip().split(":")[0].strip().upper()
        letter = LOC_LABEL_TO_LETTER.get(general_category)
        if letter:
            letters.add(letter)

    return letters_to_results(letters)


def resolve_scimago(
    area_val,
    cat_val,
    area_index: dict,
    cat_index: dict,
) -> tuple[list, list]:
    """
    Resolve LOC classes from Scimago area and category values.

    Both fields may contain multiple values separated by ";".
    Category values may include a quartile suffix like "(Q1)" which is removed
    before lookup. Both areas and categories are searched independently and
    their matched LOC letters are combined.
    """
    letters = set()

    def split_field(val) -> list[str]:
        if pd.isna(val):
            return []
        return [v.strip() for v in str(val).split(";") if v.strip()]

    def strip_quartile(cat: str) -> str:
        return re.sub(r"\s*\(Q\d+\)\s*$", "", cat).strip()

    for area in split_field(area_val):
        letters |= area_index.get(area.lower(), set())

    for cat in split_field(cat_val):
        letters |= cat_index.get(strip_quartile(cat).lower(), set())

    return letters_to_results(letters)


def resolve_row(
    row: pd.Series,
    prefix: str,
    area_index: dict,
    cat_index: dict,
) -> tuple[str | None, str | None]:
    """
    Resolve the LOC main class for a single publication (citing or cited).

    Priority order:
        1. Scimago (scimago_area and scimago_category)
        2. DOAJ    (doaj_subject) — used as primary if no Scimago data,
                                    or as fallback if Scimago produced no match

    Version 2: if any Scimago field contains "multidisciplinary", the row is
    labelled "Multidisciplinary" immediately, before any LOC lookup is attempted.
    If data is present but no LOC match is found, the row is labelled "Others"
    instead of being left unresolved. Only rows with no data at all return None
    (discarded).
    """
    scimago_area = row.get(f"{prefix}_scimago_area")
    scimago_cat  = row.get(f"{prefix}_scimago_category")
    doaj_subject = row.get(f"{prefix}_doaj_subject")

    has_scimago = not pd.isna(scimago_area) or not pd.isna(scimago_cat)
    has_doaj    = not pd.isna(doaj_subject)

    # Version 2: check for "multidisciplinary" in Scimago fields before lookup
    all_scimago_vals = " | ".join(
        str(v) for v in [scimago_area, scimago_cat] if not pd.isna(v)
    ).lower()
    if "multidisciplinary" in all_scimago_vals:
        return "Multidisciplinary", "Multidisciplinary"

    if has_scimago:
        labels, uris = resolve_scimago(scimago_area, scimago_cat, area_index, cat_index)
    elif has_doaj:
        labels, uris = resolve_doaj(doaj_subject)
    else:
        return None, None

    if not labels and has_doaj:
        labels, uris = resolve_doaj(doaj_subject)

    # Version 2: data was present but no LOC match found -> label as "Others"
    if not labels:
        return "Others", "Others"

    return format_result(labels), format_result(uris)


def main():
    print("Loading input files...")
    df = pd.read_csv(CSV_PATH)
    area_index, cat_index = load_scopus_index(JSON_PATH)
    print(f"JSON index built: {len(area_index)} area labels, {len(cat_index)} category labels")
    print(f"CSV loaded: {len(df)} rows")

    citing_labels, citing_uris = [], []
    cited_labels,  cited_uris  = [], []

    for _, row in df.iterrows():
        il, iu = resolve_row(row, "citing", area_index, cat_index)
        cl, cu = resolve_row(row, "cited",  area_index, cat_index)
        citing_labels.append(il); citing_uris.append(iu)
        cited_labels.append(cl);  cited_uris.append(cu)

    df["citing_loc_label"] = citing_labels
    df["citing_loc_uri"]   = citing_uris
    df["cited_loc_label"]  = cited_labels
    df["cited_loc_uri"]    = cited_uris

    # Version 2: count Multidisciplinary and Others across both citing and cited columns
    all_labels = citing_labels + cited_labels
    n_multi    = sum(l == "Multidisciplinary" for l in all_labels)
    n_others   = sum(l == "Others"            for l in all_labels)
    print(f"  Multidisciplinary : {n_multi} columns")
    print(f"  Others            : {n_others} columns")

    both_resolved = df["citing_loc_label"].notna() & df["cited_loc_label"].notna()
    df_resolved   = df[both_resolved]
    df_missing    = df[~both_resolved]

    out_path  = os.path.join(os.getcwd(), "SNS/output_cat.csv")
    miss_path = os.path.join(os.getcwd(), "SNS/no_match/miss_loc.csv")

    df_resolved.to_csv(out_path,  index=False)
    df_missing.to_csv(miss_path,  index=False)

    total = len(df)
    print(f"\nDone.")
    print(f"  output_cat.csv : {len(df_resolved)} rows ({len(df_resolved)/total:.1%}) — both sides resolved")
    print(f"  miss_loc.csv   : {len(df_missing)} rows ({len(df_missing)/total:.1%}) — at least one side missing")


if __name__ == "__main__":
    main()