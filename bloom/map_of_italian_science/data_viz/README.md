# Data Visualizations & Analysis

This folder contains Jupyter notebooks for the visualization and analysis 
of citation networks in the **1a. Map of Italian Science** study. 
The notebooks process data for six Italian research 
institutions — UNIBO, UNIMI, UNIPD, UNITO, UPO, and SNS — to map 
their international citation relationships at both country and 
organization level.

## Notebooks

### `country_level_analysis.ipynb`
Country-level analysis of incoming and outgoing citation flows. Covers:
- Diverging bar charts comparing incoming vs. outgoing flows per institution
- Deviation heatmap showing each institution's geographic specialization 
  relative to the Italian average
- Asymmetry map and comparison
- Cross-institution volume and diversity comparison

### `organization_level_analysis.ipynb`
Organization-level analysis of citation partnerships. Covers:
- Butterfly charts showing top-20 bilateral partners per institution
- Reciprocity scatter plots mapping citation balance across partner organizations
- Analysis of Italian institution inclusion/exclusion and its effect on 
  the network structure
- Top-10 Partner Composition with stacked proportional bar chart

### `countries_organizations_analysis.ipynb`
Combined analysis linking country-level patterns to specific organizations. Covers:
- CR₃ concentration metric: share of a country's citations driven by its top-3 organisations
- Aggregate scatter plot: citation volume vs. concentration across partner countries
- Animated scatter plot: how the volume–concentration relationship evolves across five 5-year blocks (2001–2025)
- CR₃ snapshot comparison table: 2001–2005 vs 2021–2025 across all institutions
- Sunburst charts: organisational composition of each country's citation contribution


## Setup

Install dependencies and ensure the raw citation data is available 
in `data/citation_counts/` and `data/citation_counts_annualized/` (see project root README for data access).

```bash
pip install -r requirements.txt
jupyter notebook
```

Notebooks export visualizations to two locations:
- `data/visualizations/` — tables and static images for the paper
- `website/assets/visualizations/` — interactive charts for the project website