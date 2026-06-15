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


## Source Code (`src/`)

The `src/` directory contains modularized Python scripts used across the Jupyter notebooks for data loading, cleaning, and validation:

### `data_utils.py`
Provides core utility functions to handle the citation datasets. Covers:
- Dataset loading and merging logic for country, organization, and temporal data.
- Normalization pipelines to standardize country codes/names, map territories to parent countries, and aggregate counts.
- Helper transformations, such as pivoting data for wide-format visualization and converting ISO alpha-2 to alpha-3 codes.

### `validation.py`
Provides quality-control and export functions. Covers:
- Utilities to scan and discover country name variants and duplicates.
- Validation functions to check cleaned datasets for duplicates and missing values.
- Routines to export the cleaned and aggregated datasets to CSV for both overall and temporal data.

## Setup

Install dependencies and ensure the raw citation data is available 
in `data/citation_counts/` and `data/citation_counts_annualized/` (see project root README for data access).

```bash
pip install -r requirements.txt
jupyter notebook
```

Notebooks export visualizations to two locations:
- `data/visualizations/` — tables and static images for the paper
- `website/visualizations/` — interactive charts for the project website