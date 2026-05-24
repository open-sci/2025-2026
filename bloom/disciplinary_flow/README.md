# To Do
## ~Step 1: Retireve venue data using OC Meta API~
* ~To make a request with the OC API you need to get an access token from [here](https://opencitations.net/accesstoken/)~
* ~For each institution file, run the script/iris_oc_venue.py with the iris_in_oc_index.csv~
* ~save the resulting csv files for use in step 2~
  
## Step 1: Retireve venue data from OC Meta data dump
* Download oc meta datadump from [here](https://download.opencitations.net/#meta) - Dump created on 2025-06-06
* Leave tar.gz file zipped and for each institution file, run the script/iris_oc_venue_v2.py with the iris_in_oc_index.csv
* Save the resulting csv files for use in step 2
  
## Step 2: Use venue PIDs (e.g issn) to extract subject info from external data dumps (DOAJ and Scimago)
* For each institution file, run the script/PID_subject_match.py with the iris_oc_venues_matched.csv (update the paths in the CONFIGURATION block at the top)
* Run the script/disciplinary_map_sample_output.py with the disciplinary_map_matched.csv (update the path) to obtain a sample data set in .csv format.

## Step 3: Use LOC classifications to align and standardise subjects across all iris_oc resources
* For each institution file, run the [`scripts/add_loc_cat.py`](scripts/add_loc_cat.py) with the [`categories/merged_loc_scopus.json`](categories/merged_loc_scopus.json) (update the `CSV_PATH` and `JSON_PATH` at the beginning of the script).
* Two output files are expected: output_cat.csv and miss_loc.csv 

## Step 4: Aggregate data and push results to 2025-2026/1b
* For each institution file, run the [`scripts/aggregate_v2.py`](scripts/aggregate_v2.py) with the output_cat.csv (update the path of `INPUT` and `OUTPUT` at the beginning of the script).
* The script will generate a summary report in the terminal about detailed count for citation actions.
* Copy this complete report text and paste it to the end of `uni_summary.txt`.
* Push the aggregated CSV file (`uni_agg_output.csv`) and the updated `uni_summary.txt` to the [`step4_output`](categories/step4_output) folder.
  
## Step 5: Select charts and visualise the data
...
