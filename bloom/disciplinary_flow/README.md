# To Do
## ~Step 1: Retireve venue data using OC Meta API~
* ~To make a request with the OC API you need to get an access token from [here](https://opencitations.net/accesstoken/)~
* ~For each institution file, run the script/iris_oc_venue.py with the iris_in_oc_index.csv~
* ~save the resulting csv files for use in step 2 ~
## Step 1: Retireve venue data from OC Meta data dump
* Download oc meta datadump from [here](https://download.opencitations.net/#meta)
* leave tar.gz file zipped and run for each institution file, run the script/iris_oc_venue_v2.py with the iris_in_oc_index.csv
* save the resulting csv files for use in step 2
## Step 2: Use venue PIDs (e.g issn) to extract subject info from external data dumps (DOAJ and Scimago)
## Step 3: Use LOC classifications to align and standardise subjects across all iris_oc resources
