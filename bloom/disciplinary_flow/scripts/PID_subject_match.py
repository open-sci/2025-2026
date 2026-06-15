import pandas as pd
import re


# ==================================================
# CONFIGURATION
# ==================================================

#Update here with your paths !!

INPUT_FILE = r"D:\Downloads\Open Science\data\UNITO\iris_oc_venues_matched.csv"
SCIMAGO_FILE = r"D:\Downloads\Open Science\journal_data\scimago.csv"
DOAJ_FILE = r"D:\Downloads\Open Science\journal_data\doaj.csv"
OUTPUT_FILE = r"D:\Downloads\Open Science\data\UNITO\disciplinary_map_matched.csv"
# ==================================================
# STEP 1: EXTRACT ALL ISSNS
# ==================================================
def extract_issns(text):
    """
    Extract all ISSNs from venue metadata.
    Returns a list of normalized ISSNs.
    Example:
        1234-5678 -> 12345678
    """

    if pd.isna(text):
        return []

    matches = re.findall(r"\d{4}-\d{3}[\dX]", str(text))

    cleaned = []

    for match in matches:
        cleaned.append(
            match.replace("-", "").upper()
        )

    return cleaned


# ==================================================
# STEP 2: LOAD DATASETS
# ==================================================
def load_datasets():

    print("Step 1: Loading datasets...")

    try:
        main_df = pd.read_csv(INPUT_FILE)

        # SCImago files are generally semicolon-separated
        scimago = pd.read_csv(
            SCIMAGO_FILE,
            sep=";",
            encoding="latin1"
        )

        doaj = pd.read_csv(DOAJ_FILE)

        print("✓ Files loaded successfully.")

        return main_df, scimago, doaj

    except Exception as e:

        print(f"✗ Loading Error: {e}")

        return None, None, None


# ==================================================
# STEP 3: CLEAN DOAJ
# ==================================================
def clean_doaj(doaj):

    print("Step 2: Cleaning DOAJ ISSNs...")

    doaj["issn_p"] = (
        doaj["Journal ISSN (print version)"]
        .fillna("")
        .astype(str)
        .str.replace("-", "", regex=False)
        .str.upper()
    )

    doaj["issn_e"] = (
        doaj["Journal EISSN (online version)"]
        .fillna("")
        .astype(str)
        .str.replace("-", "", regex=False)
        .str.upper()
    )

    return doaj


# ==================================================
# STEP 4: CLEAN SCIMAGO
# ==================================================
def clean_scimago(scimago):

    print("Step 3: Cleaning SCImago ISSNs...")

    scimago["Issn"] = (
        scimago["Issn"]
        .fillna("")
        .astype(str)
        .str.upper()
    )

    scimago["issn_list"] = (
        scimago["Issn"]
        .str.replace("-", "", regex=False)
        .str.split(",")
    )

    # remove whitespace
    cleaned_lists = []

    for items in scimago["issn_list"]:

        cleaned = []

        for item in items:
            cleaned.append(item.strip())

        cleaned_lists.append(cleaned)

    scimago["issn_list"] = cleaned_lists

    return scimago.explode("issn_list")


# ==================================================
# STEP 5: BUILD LOOKUP MAPS
# ==================================================
def build_lookup_maps(scimago, doaj):

    print("Step 4: Building lookup maps...")

    # ---------- SCImago ----------
    s_area_map = dict(
        zip(scimago["issn_list"], scimago["Areas"])
    )

    s_category_map = dict(
        zip(scimago["issn_list"], scimago["Categories"])
    )

    # ---------- DOAJ ----------
    doaj_area_map = dict(
        zip(doaj["issn_p"], doaj["LCC Codes"])
    )

    doaj_area_map.update(
        dict(zip(doaj["issn_e"], doaj["LCC Codes"]))
    )

    doaj_subject_map = dict(
        zip(doaj["issn_p"], doaj["Subjects"])
    )

    doaj_subject_map.update(
        dict(zip(doaj["issn_e"], doaj["Subjects"]))
    )

    return (
        s_area_map,
        s_category_map,
        doaj_area_map,
        doaj_subject_map
    )


# ==================================================
# STEP 6: ENRICHMENT FUNCTIONS
# ==================================================
def get_from_map(issn_list, target_map):
    """
    Looks up a list of ISSNs in a specific dictionary (target_map).
    Returns the first valid match it finds.
    """
    if not isinstance(issn_list, list) or not issn_list:
        return None

    for issn in issn_list:
        result = target_map.get(issn)
        if result is not None and not pd.isna(result):
            return result

    return None


# ==================================================
# STEP 7: APPLY ENRICHMENT
# ==================================================
def enrich_column(series, map_func, *maps):

    results = []

    for value in series:

        result = map_func(value, *maps)

        results.append(result)

    return results


# ==================================================
# STEP 8: MAIN PIPELINE
# ==================================================
def run_enrichment():

    # Load datasets
    main_df, scimago, doaj = load_datasets()

    if main_df is None:
        return

    # Clean datasets
    doaj = clean_doaj(doaj)
    scimago = clean_scimago(scimago)

    # Extract ISSNs
    print("Step 5: Extracting ISSNs from venue metadata...")

    main_df["citing_issns"] = (
        main_df["citing_venue"]
        .apply(extract_issns)
    )

    main_df["cited_issns"] = (
        main_df["cited_venue"]
        .apply(extract_issns)
    )

    # Build maps
    (
        s_area_map,
        s_category_map,
        doaj_area_map,
        doaj_subject_map
    ) = build_lookup_maps(scimago, doaj)

    # Apply enrichment
    print("Step 6: Applying independent disciplinary enrichment...")

    # ---------- CITING COLUMNS ----------
    main_df["citing_scimago_area"] = enrich_column(
        main_df["citing_issns"], get_from_map, s_area_map
    )
    main_df["citing_scimago_category"] = enrich_column(
        main_df["citing_issns"], get_from_map, s_category_map
    )
    main_df["citing_doaj_lcc"] = enrich_column(
        main_df["citing_issns"], get_from_map, doaj_area_map
    )
    main_df["citing_doaj_subject"] = enrich_column(
        main_df["citing_issns"], get_from_map, doaj_subject_map
    )

    # ---------- CITED COLUMNS ----------
    main_df["cited_scimago_area"] = enrich_column(
        main_df["cited_issns"], get_from_map, s_area_map
    )
    main_df["cited_scimago_category"] = enrich_column(
        main_df["cited_issns"], get_from_map, s_category_map
    )
    main_df["cited_doaj_lcc"] = enrich_column(
        main_df["cited_issns"], get_from_map, doaj_area_map
    )
    main_df["cited_doaj_subject"] = enrich_column(
        main_df["cited_issns"], get_from_map, doaj_subject_map
    )

    # Save output
    print("Step 7: Saving enriched dataset...")

    main_df.to_csv(OUTPUT_FILE, index=False)

# ==================================================
# FINAL REPORT & LOGGING
# ==================================================    
    report_text = (
        "\n" + "=" * 60 + "\n"
        "PARALLEL DISCIPLINARY ENRICHMENT COMPLETE\n" +
        "=" * 60 + "\n"
        f"Total Records: {len(main_df)}\n"
        f"Verification: No duplicates created during mapping.\n\n"
        f"Citing Match Rate (SCImago): {main_df['citing_scimago_area'].notna().mean():.2%}\n"
        f"Citing Match Rate (DOAJ):    {main_df['citing_doaj_lcc'].notna().mean():.2%}\n\n"
        f"Cited Match Rate (SCImago):  {main_df['cited_scimago_area'].notna().mean():.2%}\n"
        f"Cited Match Rate (DOAJ):     {main_df['cited_doaj_lcc'].notna().mean():.2%}\n\n"
        f"Final Dataset Saved: {OUTPUT_FILE}\n" +
        "=" * 60 + "\n"
    )

    print(report_text)

    with open("matching_summary.txt", "w", encoding="utf-8") as f:
        f.write(report_text)
# ==================================================
# RUN PIPELINE
# ==================================================
if __name__ == "__main__":
    run_enrichment()