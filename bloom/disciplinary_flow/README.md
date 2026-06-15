# 1b. Disciplinary Flow
The following workflow is used to enrich the [IRIS dataset](https://doi.org/10.5281/zenodo.18202530) with subject classification information in order to analyse and visualise disciplinary flow trends between the citation entities of each the six Italian institutions. 

This analysis begins with the enrichement of the dataset of IRIS aligned with the OpenCitation Index which contains every citation entity made from the publication of each IRIS institution. The scripts for this analysis were designed to be run on each institution file individually.

The workflow is divided into four steps. Step 1 and Step 2 handle the data enrichment with the OpenCitations Meta data dump and external journal datasets (Scimago and DOAJ) for subjecting information. Step 3 uses the Library of Congress subject classification for disciplinary alignment and standardisation. Step 4 and Step 5 handle the aggregation, analysis and visualisation of the newly enriched dataset. 

Before starting the workflow download the original dataset above.

## Step 1: Retireve venue data from OC Meta data dump
* Download oc meta datadump from [here](https://download.opencitations.net/#meta) - Dump created on 2025-06-06
* Leave tar.gz file zipped and for each institution file, run the [`script/iris_oc_venue_v2.py`](script/iris_oc_venue_v2.py) with the iris_in_oc_index.csv from the orignal IRIS dataset.
* Save the resulting csv files for use in step 2
  
## Step 2: Use venue PIDs (e.g issn) to extract subject info from external data dumps (DOAJ and Scimago)
* For each institution file, run the [`script/PID_subject_match_v3.py`](script/PID_subject_match_v3.py) with the iris_oc_venues_matched.csv from Step 1 (update the paths in the CONFIGURATION block at the top)
* To obtain a sample data set in .csv format run the [`script/disciplinary_map_sample_output.py`](script/disciplinary_map_sample_output.py) with the disciplinary_map_matched.csv (update the path).

## Step 3: Use LOC classifications to align and standardise subjects across all iris_oc resources
* For each institution file, run the [`scripts/add_loc_cat_v2.py`](scripts/add_loc_cat_v2.py) with the [`categories/merged_loc_scopus.json`](categories/merged_loc_scopus.json) (update the `CSV_PATH` and `JSON_PATH` at the beginning of the script).
* Two output files are expected: output_cat.csv and miss_loc.csv 

## Step 4: Aggregate data
* For each institution file, run the [`scripts/aggregate_v3.py`](scripts/aggregate_v3.py) with the output_cat.csv (update the path of `INPUT`, `OUTPUT`and `PROFILE_OUTPUT_FILE` at the beginning of the script).
* This script will generate a [inst_name]_agg_output.csv file aggregating all the citation discipline pairs giving a weight to them based on frequency. 
* It will also generate a summary report in the terminal with a detailed count for citation actions.
  
## Step 5: Visualization and analysis
 * Open [`visualization/viz_discipline_flow.ipynb`](disciplinary_flow/viz_discipline_flow.ipynb) in Jupyter or Google Colab.
* The notebook reads the `*_profile_output.csv` and `*_agg_output.csv` files directly from the `step4_output` folder via GitHub raw URLs.
* Run all cells in order. The notebook is structured into three analyses:
  * **Analysis 1 - Overall Disciplinary Distribution:** Butterfly charts, grouped bar charts, and 100% stacked bar charts showing the volume and proportional share of disciplines citing and cited by each institution.
  * **Analysis 2 - Disciplinary Citation Flow:** Citing–cited pair rankings, self vs. cross citation breakdowns, sunburst charts, and static/interactive Sankey diagrams showing directed flows between discipline pairs.
  * **Analysis 3 - Cross-Institutional Comparison:** A relative disciplinary specialisation heatmap showing how each institution's disciplinary profile deviates from the cross-institution average.
