"""
DESCRIPTION
------------
This script aggregates disciplinary flow data for individual institutions. It takes the "citing_loc_label", "cited_loc_label", and "flow" columns from the input. 
It aggregates the data to create an edge list with weights representing the number of citation actions between different disciplines, broken down by flow type (Incoming, Outgoing, Internal).
It supports two distinct execution modes via user interaction:
  Mode [0]: Generates both outputs.
  Mode [1]: Skips the edge list and instantly updates the Subject Profile.

OUTPUTS
-----------------
1. unipd_disciplinary_edges.csv: 
   An edge list [flow, citing_loc_label, cited_loc_label, weight] sorted by weight.
   Designed for Sankey / Chord diagrams.

2. unipd_subject_profile.csv:
   An institutional profile matrix [Discipline, cited_count, citing_count, total_count] 
   sorted by total activity. Ideal for comparative bar charts.

3. Aggregation Report (printed to console):
   Summary of input rows, flow type counts before/after expansion.

AGGREGATION METHOD
------------------
We use the "Integer Counting" method to calculate discipline-to-discipline citations.

If a journal belongs to multiple disciplines (separated by ' | '), we split them and expand them into all possible pairs (Cartesian product). Each pair counts as 1 full citation action, inheriting the original flow type.

E.g., An 'Incoming' citation where a Medicine | Science journal cites a Philosophy | History journal is broken down into 4 separate actions (each weight = 1, flow = Incoming):
    1. Incoming | Medicine -> Philosophy
    2. Incoming | Medicine -> History
    3. Incoming | Science  -> Philosophy
    4. Incoming | Science  -> History

Finally, it groups all identical combinations (same flow, same citing, same cited) together and sums up their weights.
"""

import pandas as pd

INPUT_FILE = "SNS/output_cat.csv"
OUTPUT_FILE = "SNS/sns_agg_output.csv"
PROFILE_OUTPUT_FILE = "SNS/sns_profile_output.csv"

def aggregation():
    print("Start: Choose Module:")
    print("[0] Run all")
    print("[1] Skip the first output, generating the Citing and Cited separate tables directly.")
    print("-"*50)
    choice = input("Enter your choice (0 or 1): ").strip()

    # ==================================================
    # Load, Clean, Split
    # ==================================================
    print("Loading data...")
    # read csv
    df = pd.read_csv(INPUT_FILE, usecols=['citing_loc_label', 'cited_loc_label', 'flow'])
    total_input_rows = len(df)
    print(f"Total input: {total_input_rows:,} rows.")
    
    # statitic the number of Nans
    missing_mask = (
        df['citing_loc_label'].isna() | 
        df['cited_loc_label'].isna() | 
        df['flow'].isna() |
        (df['citing_loc_label'].astype(str).str.strip() == '') | 
        (df['cited_loc_label'].astype(str).str.strip() == '') |
        (df['citing_loc_label'].astype(str).str.strip() == '|') |
        (df['cited_loc_label'].astype(str).str.strip() == '|')
    )
    num_missing_rows = missing_mask.sum()
    num_usable_rows = total_input_rows - num_missing_rows
    print(f"Invalid data: {num_missing_rows/total_input_rows:.2%}, {num_missing_rows:,} rows")
    print(f"Usable data: {num_usable_rows/total_input_rows:.2%}, {num_usable_rows:,} rows")

    df_clean = df[~missing_mask].copy()

    # ==================================================
    # Count flow types BEFORE expansion
    # ==================================================
    flow_counts_before = df_clean['flow'].value_counts()
    count_incoming_before = flow_counts_before.get('Incoming', 0)
    count_outgoing_before = flow_counts_before.get('Outgoing', 0)
    count_internal_before = flow_counts_before.get('Internal', 0)

    # deal with multiple labels, split by " | "
    df_clean['citing_loc_label'] = df_clean['citing_loc_label'].str.split(' | ', regex=False)
    df_clean['cited_loc_label'] = df_clean['cited_loc_label'].str.split(' | ', regex=False)

    # divide and aggregate
    df_exploded = df_clean.explode('citing_loc_label').explode('cited_loc_label')
    total_citation_actions = len(df_exploded)

    # ==================================================
    # Count flow types AFTER expansion
    # ==================================================
    flow_counts_after = df_exploded['flow'].value_counts()
    count_incoming_after = flow_counts_after.get('Incoming', 0)
    count_outgoing_after = flow_counts_after.get('Outgoing', 0)
    count_internal_after = flow_counts_after.get('Internal', 0)

    if choice == '1':
        print("Skipping the first output. Generating Citing and Cited separate tables directly...")
    else:
        # count, aggregate and rank by the weight
        edge_list = df_exploded.groupby(['flow','citing_loc_label', 'cited_loc_label']).size().reset_index(name='weight')
        edge_list = edge_list.sort_values(by='weight', ascending=False)
        edge_list.to_csv(OUTPUT_FILE, index=False)

        print(f"[Success] Disciplinary Edge table is saved at: {OUTPUT_FILE}")

    # --------------------------------------------------
    # Generate Aggregation Report
    # --------------------------------------------------
    print("\n" + "=" * 70 + "\n"
          "AGGREGATION REPORT\n" +
          "=" * 70 + "\n")
    
    print(f"Total input rows: {num_usable_rows:,}\n")
    
    print("BEFORE EXPANSION (Input File Counts):")
    print(f"  Incoming:  {count_incoming_before:,} ({count_incoming_before/num_usable_rows:.2%})")
    print(f"  Outgoing:  {count_outgoing_before:,} ({count_outgoing_before/num_usable_rows:.2%})")
    print(f"  Internal:  {count_internal_before:,} ({count_internal_before/num_usable_rows:.2%})")
    print(f"  Total:     {num_usable_rows:,}\n")
    
    print("AFTER EXPANSION (Individual Discipline Citations):")
    print(f"  Incoming:  {count_incoming_after:,} ({count_incoming_after/total_citation_actions:.2%})")
    print(f"  Outgoing:  {count_outgoing_after:,} ({count_outgoing_after/total_citation_actions:.2%})")
    print(f"  Internal:  {count_internal_after:,} ({count_internal_after/total_citation_actions:.2%})")
    print(f"  Total:     {total_citation_actions:,}\n")
    
    expansion_ratio = total_citation_actions / num_usable_rows
    print(f"Expansion Ratio: {expansion_ratio:.2f}x")
    print("=" * 70 + "\n")

    # --------------------------------------------------
    # Output 2 
    # --------------------------------------------------
    print("Structuring the Subject Profile Table (Discipline, cited_count, citing_count, total_count)...")

    # 1. Count Cited: (Incoming and Internal)
    df_cited_side = df_exploded[df_exploded['flow'].isin(['Incoming', 'Internal'])]
    cited_counts = df_cited_side.groupby('cited_loc_label').size().reset_index(name='cited_count')
    cited_counts.rename(columns={'cited_loc_label': 'Discipline'}, inplace=True)

    # 2. Count Citing: (Outgoing and Internal)
    df_citing_side = df_exploded[df_exploded['flow'].isin(['Outgoing', 'Internal'])]
    citing_counts = df_citing_side.groupby('citing_loc_label').size().reset_index(name='citing_count')
    citing_counts.rename(columns={'citing_loc_label': 'Discipline'}, inplace=True)

    subject_profile = pd.merge(cited_counts, citing_counts, on='Discipline', how='outer').fillna(0)
    
    subject_profile['cited_count'] = subject_profile['cited_count'].astype(int)
    subject_profile['citing_count'] = subject_profile['citing_count'].astype(int)

    subject_profile['total_count'] = subject_profile['cited_count'] + subject_profile['citing_count']
    subject_profile = subject_profile.sort_values(by='total_count', ascending=False)

    # PROFILE_OUTPUT_FILE = "unipd_subject_profile.csv"
    subject_profile.to_csv(PROFILE_OUTPUT_FILE, index=False)
    print(f"Done. Saved the subject profile table at: {PROFILE_OUTPUT_FILE}")


if __name__ == "__main__":
    aggregation()

