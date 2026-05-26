# 1a. Map of Italian Science

The goal of this project is to create a comprehensive mapping of the Italian
scientific landscape, starting from the IRIS dataset and enriching it with data
from OpenCitations, OpenAIRE and ROR. The final output will be a mapping of
publications to organizations and countries, as well as the number of inbound
and outbound citations for each publication.

## Overview

The mapping process is divided into four main steps:
1. Mapping of IRIS publications to OpenCitations PIDs (DOI, PMID, ISBN).
2. Mapping of OpenAIRE organizations to ROR identifiers, names and countries.
3. Mapping of IRIS publications to OpenAIRE organizations using the PIDs from
   step 1 and the relations dump from OpenAIRE.
4. Counting of inbound and outbound citations for each publication, producing
   aggregated statistics for each IRIS university for both organizations and
   countries.

## Process

### Data

#### 1. Preparation of the storage location
To execute all the steps of the pipeline from scratch, a storage location with
at least 350GB of space is required, as it will be used to store all the data
dumps from IRIS, OpenCitations, OpenAIRE and ROR, as well as the intermediate
and final outputs of the pipeline.

#### 2. Download all the data dumps

We will need data from IRIS, OpenCitations, OpenAIRE and ROR to execute the
mapping pipeline. This project was developed and tested with the following
versions of the data dumps:

| dump          | version    | url                                 |
|---------------|------------|-------------------------------------|
| IRIS          | 1.1.0      | https://zenodo.org/records/18202530 |
| ROR           | 2.7        | https://zenodo.org/records/20140273 |
| OpenCitations | 2026-01-14 | https://zenodo.org/records/18324537 |
| OpenAIRE      | 10.6.0     | https://zenodo.org/records/17725827 |

Create the directories for the dumps:

```bash
mkdir -p data/dumps
```

Download and extract the **IRIS** data dump from Zenodo:

```bash
curl -LO --output-dir data/dumps "https://zenodo.org/records/18202530/files/data.zip?download=1"
unzip data/dumps/data.zip -d data/dumps/iris
rm data/dumps/data.zip
```

Download and extract the **ROR** data dump from Zenodo:

```bash
curl -LO --output-dir data/dumps "https://zenodo.org/records/20140273/files/v2.7-2026-05-12-ror-data.zip?download=1"
unzip data/dumps/v2.7-2026-05-12-ror-data.zip -d data/dumps/ror
rm data/dumps/v2.7-2026-05-12-ror-data.zip
```

Download the **OpenCitations** Meta CSV dump from Zenodo:

```bash
curl -LO --create-dirs --output-dir data/dumps/opencitations "https://zenodo.org/records/18324537/files/output_csv_2026_01_14.tar.gz?download=1"
```

Download the **OpenAIRE** publications, relations and organizations data dumps
from Zenodo:

```bash
curl -L --remote-name-all --create-dirs --output-dir data/dumps/openaire \
  "https://zenodo.org/records/17725827/files/organization.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_1.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_2.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_3.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_4.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_5.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_6.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_7.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_8.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_9.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_10.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_11.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_12.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_13.tar?download=1" \
  "https://zenodo.org/records/17725827/files/publication_14.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_1.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_2.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_3.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_4.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_5.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_6.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_7.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_8.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_9.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_10.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_11.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_12.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_13.tar?download=1" \
  "https://zenodo.org/records/17725827/files/relation_14.tar?download=1"
```

---

### Code

#### 3. Prepare the Python environment

Create a Python virtual environment and install the required dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

#### 4. Run the IRIS/OpenCitations pids mapping script

The first step of the pipeline will extend each IRIS publication from the
initial dataset with the PIDs (DOI, PMID, ISBN) from the corresponding
publication in OpenCitations, if available.

This will also produce an index of unique entries across all IRIS publications,
which will be used to speed up the search for publications in OpenAIRE.

```bash
python src/build_iris_oc_pids.py
```

###### Files produced

Data path prefix: `iris_oc_pids/`

| file | description |
|---|---|
| `<university_name>/iris_oc_pids.csv` | Contains the mapping of IRIS publications to OpenCitations PIDs for a specific university. |
| `<university_name>/iris_oc_pids.missing.csv` | Contains the IRIS publications for which no OpenCitations PIDs were found for a specific university. |
| `<university_name>/iris_oc_pids.metadata.json` | Contains metadata about the IRIS to OpenCitations PIDs mapping process for a specific university. |
| `unique_pids.csv` | Contains a list of unique PIDs across all IRIS publications. |
| `unique_pids.metadata.json` | Contains metadata about the unique PIDs generation process across all IRIS publications. |

#### 5. Run the OpenAIRE/ROR mapping script
The second step of the pipeline will create a mapping between the organizations
in OpenAIRE and ROR, resolving duplicates and inconsistencies, and selecting a
canonical name for each organization. 

This mapping will be used in the next step of the pipeline to quickly lookup the
OpenAIRE organization id and retrieve the precise name and country.

```bash
python src/match_organizations_countries.py
```

###### Files produced

Data path prefix: `openaire_ror_countries/`

| file | description |
|---|---|
| `openaire_ror_countries.json` | Contains the mapping of OpenAIRE organizations to ROR identifiers, names and countries. |
| `openaire_ror_countries.metadata.json` | Contains metadata about the OpenAIRE to ROR mapping process. |

#### 6. Run the IRIS/OpenAIRE mapping script

The third step of the pipeline is the core of the mapping process, as it will
create a mapping between the IRIS publications and the OpenAIRE publications
using the PIDs produced in step one, resolving authors' affiliations via the
relations dump and producing a final mapping between publications and
organizations involved.

This step is very computationally intensive, as it requires iterating over all
the OpenAIRE publications and relations tar files, and will take several hours
to complete.

```bash
python src/resolve_pids_organizations.py
```

###### Files produced

Data path prefix: `iris_openaire_organizations/`

| file | description |
|---|---|
| `missing_no_searchable_pid.csv` | Contains the IRIS publications for which no searchable PID was found. |
| `omid_organizations.json` | Contains the mapping of IRIS publications to OpenAIRE organizations. |
| `omid_organizations.metadata.json` | Contains metadata about the IRIS to OpenAIRE organizations mapping process. |

#### 7. Run the inbound/outbound citations counting script
The fourth and final step of the pipeline will count the number of inbound and
outbound citations for each publication, producing aggregated statistics for
each IRIS university for both organizations and countries.

```bash
python src/count_citations.py
```

###### Files produced

Data path prefix: `citation_counts/`

| file | description |
|---|---|
| `<university_name>/citation_counts.metadata.json` | Contains metadata about the citation counts process for a specific university. |
| `<university_name>/citation_counts_countries_inbound.csv` | Contains the inbound citation counts for countries for a specific university. |
| `<university_name>/citation_counts_countries_outbound.csv` | Contains the outbound citation counts for countries for a specific university. |
| `<university_name>/citation_counts_organizations_inbound.csv` | Contains the inbound citation counts for organizations for a specific university. |
| `<university_name>/citation_counts_organizations_outbound.csv` | Contains the outbound citation counts for organizations for a specific university. |
