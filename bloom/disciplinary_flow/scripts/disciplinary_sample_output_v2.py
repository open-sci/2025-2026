import pandas as pd

# ==================================================
# LOAD DATASETS
# ==================================================

# Update paths if needed

MATCHED_FILE = r"D:\Downloads\Open Science\data\UNITO\disciplinary_map_matched.csv"

NO_MATCH_FILE = r"D:\Downloads\Open Science\data\UNITO\disciplinary_map_no_match.csv"

NO_ISSN_FILE = r"D:\Downloads\Open Science\data\UNITO\disciplinary_map_no_issn.csv"

# ==================================================
# READ CSV FILES
# ==================================================

print("Loading datasets...")

matched_df = pd.read_csv(MATCHED_FILE)

unmatched_df = pd.read_csv(NO_MATCH_FILE)

no_issn_df = pd.read_csv(NO_ISSN_FILE)

print("Datasets loaded successfully.")

# ==================================================
# CREATE RANDOM SAMPLES
# ==================================================

print("Creating random samples...")

matched_sample = matched_df.sample(
    n=min(1000, len(matched_df)),
    random_state=42
)

unmatched_sample = unmatched_df.sample(
    n=min(1000, len(unmatched_df)),
    random_state=42
)

no_issn_sample = no_issn_df.sample(
    n=min(1000, len(unmatched_df)),
    random_state=42
)

# ==================================================
# SAVE TO EXCEL
# ==================================================

print("Saving Excel files...")

matched_sample.to_excel(
    "disciplinary_map_sample_output.xlsx",
    index=False
)

unmatched_sample.to_excel(
    "disciplinary_map_nomatch_sample_output.xlsx",
    index=False
)

no_issn_sample.to_excel(
    "disciplinary_map_noissn_sample_output.xlsx",
    index=False
)

# ==================================================
# FINAL REPORT
# ==================================================

print("\n" + "=" * 50)

print("RANDOM SAMPLES CREATED SUCCESSFULLY")

print("=" * 50)

print(f"Matched Sample Size: {len(matched_sample)}")

print(f"Unmatched Sample Size: {len(unmatched_sample)}")

print(f"No ISSN Sample Size: {len(no_issn_sample)}")

print("\nFiles created:")

print("- disciplinary_map_sample_output.xlsx")

print("- disciplinary_map_nomatch_sample_output.xlsx")

print("- disciplinary_map_noissn_sample_output.xlsx")

print("=" * 50)