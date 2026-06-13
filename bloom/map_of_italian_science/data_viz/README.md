# Data Visualizations & Analysis

This folder contains Jupyter notebooks for the visualization and analysis 
of citation networks in the **1a. Map of Italian Science** project. 
The notebooks process data for six Italian research 
institutions — UNIBO, UNIMI, UNIPD, UNITO, UPO, and SNS — to map 
their international citation relationships at both country and 
organization level.

## Notebooks

### `countries_analysis.ipynb`
Country-level analysis of inbound and outbound citation flows. Covers:
- Data cleaning pipeline and canonical country name mapping
- Diverging bar charts comparing inbound vs. outbound flows per institution
- Deviation heatmap showing each institution's geographic specialization 
  relative to the Italian average
- Choropleth and asymmetry maps (UNIBO deep-dive)
- Cross-institution volume and diversity comparison

### `organizations_analysis.ipynb`
Organization-level analysis of citation partnerships. Covers:
- Butterfly charts showing top-20 bilateral partners per institution
- Reciprocity scatter plots mapping citation balance across partner organizations
- Analysis of Italian institution inclusion/exclusion and its effect on 
  the network structure

### `countries_organisations_analysis.ipynb`
Combined analysis linking country-level patterns to specific organizations. Covers:
- Citation concentration metric: how much of a country's citation volume 
  is driven by its top-N organizations
- Sankey diagrams mapping flows from Italian institutions → partner 
  countries → specific foreign organizations

## Setup

Install dependencies and ensure the raw citation data is available 
in `data/citation_counts/` (see project root README for data access).

```bash
pip install -r requirements.txt
jupyter notebook
```

Open notebooks in the order listed above.