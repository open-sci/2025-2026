import pandas as pd

# ==================================================
# LOAD DATASET
# ==================================================

INPUT_FILE = r"D:\Downloads\Open Science\data\UNITO\output_cat.csv"

OUTPUT_FILE = r"D:\Downloads\Open Science\data\UNITO\final_output_cat.csv"

df = pd.read_csv(INPUT_FILE)

# ==================================================
# RENAME URI COLUMNS
# ==================================================

df = df.rename(columns={
    "citing": "citing_uri",
    "cited": "cited_uri"
})

# ==================================================
# KEEP ONLY FINAL ANALYTICAL COLUMNS
# ==================================================

final_columns = [
    "id",
    "citing_uri",
    "cited_uri",
    "citing_venue",
    "cited_venue",
    "citing_loc_label",
    "citing_loc_uri",
    "cited_loc_label",
    "cited_loc_uri",
    "flow"
]

final_df = df[final_columns]

# ==================================================
# SAVE FINAL OUTPUT
# ==================================================

final_df.to_csv(
    OUTPUT_FILE,
    index=False
)

# ==================================================
# REPORT
# ==================================================

print("\n" + "=" * 50)

print("FINAL OUTPUT CREATED SUCCESSFULLY")

print("=" * 50)

print(f"Total Records: {len(final_df)}")

print(f"Output File: {OUTPUT_FILE}")

print("=" * 50)