import pandas as pd

# Load large CSV

#Change the path according to your "disciplinary_map_matched.csv"

df = pd.read_csv(r"D:\Downloads\Open Science\data\UNITO\disciplinary_map_matched.csv")

# Random sample of 1000 rows
sample_df = df.sample(n=1000, random_state=42)

# Save as Excel
sample_df.to_excel("disciplinary_map_sample_output.xlsx", index=False)

print("Random sample Excel file created: disciplinary_map_sample_output.xlsx")