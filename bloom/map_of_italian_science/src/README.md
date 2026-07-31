# 1a. Map of Italian Science

The goal of this project is to create a comprehensive mapping of the Italian
scientific landscape, starting from the IRIS dataset and enriching it with data
from OpenCitations, OpenAIRE and ROR. The final output will be a mapping of
publications to organizations and countries, as well as the number of incoming
and outgoing citations for each publication.

## Overview

The mapping process is divided into four main steps:
1. Mapping of IRIS publications to OpenCitations PIDs (DOI, PMID, ISBN).
2. Building a ROR-keyed organization index, and resolving each OpenAIRE
   organization to a single ROR identifier.
3. Mapping of IRIS publications to OpenAIRE organizations using the PIDs from
   step 1 and the relations dump from OpenAIRE, then resolving those
   organizations to their ROR records.
4. Counting of incoming and outgoing citations for each publication, producing
   aggregated statistics for each IRIS university for both organizations and
   countries.

ROR is the sole authority on organization identity: every organization name,
identifier and country in the final output comes from the ROR dump, and
organizations that cannot be resolved to exactly one ROR record are excluded.

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

| dump          | version    | url                                     |
|---------------|------------|-----------------------------------------|
| IRIS          | 1.1.0      | https://doi.org/10.5281/zenodo.18202530 |
| ROR           | 2.7        | https://doi.org/10.5281/zenodo.20140273 |
| OpenCitations | 2026-01-14 | https://doi.org/10.5281/zenodo.18324537 |
| OpenAIRE      | 10.6.0     | https://doi.org/10.5281/zenodo.17725827 |

Create the directories for the dumps:

```bash
mkdir -p data/dumps
```

Download and extract the **IRIS** data dump from Zenodo:

```bash
curl -LO --output-dir data/dumps "https://zenodo.org/records/18202530/files/data.zip?download=1"
unzip data/dumps/data.zip -d data/dumps/iris_publications
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

The second step of the pipeline establishes ROR as the authority on organization
identity. It produces two files: an index of every ROR organization keyed by ROR
identifier and a lookup resolving each OpenAIRE organization id to a single ROR
identifier.

The second file is needed only because OpenAIRE's affiliation edges reference
OpenAIRE organization ids; those ids are never published in the final output.

OpenAIRE frequently attaches several ROR identifiers to one organization record,
expressing its own uncertainty about which entity the record describes rather
than listing several affiliations. An organization is therefore included only if
it resolves to exactly one ROR identifier: where several are attached,
OpenAIRE's `legalName` and country are used *solely* to choose between the ROR
candidates, and the organization is dropped when they single out none.

```bash
python src/match_organizations_countries.py
```

###### Files produced

Data path prefix: `openaire_ror_countries/`

| file | description |
|---|---|
| `ror_organizations.json` | Contains every ROR organization keyed by ROR identifier, with its name, country name and country code. Every value published downstream comes from this file. |
| `openaire_ror_map.json` | Contains the mapping of each resolvable OpenAIRE organization id to its single ROR identifier. |
| `ror_organizations.metadata.json` | Contains metadata about the OpenAIRE to ROR mapping process, including how many organizations were dropped and why. |

#### 6. Run the IRIS/OpenAIRE mapping script

The third step of the pipeline is the core of the mapping process, as it will
create a mapping between the IRIS publications and the OpenAIRE publications
using the PIDs produced in step one, resolving authors' affiliations via the
relations dump and producing a final mapping between publications and
organizations involved.

Each affiliated organization is resolved in two hops — OpenAIRE organization id
to ROR identifier, then ROR identifier to its ROR record — using the two files
produced in step two. Organizations absent from that mapping are dropped, and
the organizations of each publication are deduplicated on their ROR identifier,
so that several OpenAIRE records describing the same institution are counted
once.

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

#### 7. Run the incoming/outgoing citations counting script
The fourth and final step of the pipeline will count the number of incoming and
outgoing citations for each publication, producing aggregated statistics for
each IRIS university for both organizations and countries.

```bash
python src/count_citations.py
```

###### Files produced

Data path prefix: `citation_counts/`

| file | description |
|---|---|
| `<university_name>/citation_counts.metadata.json` | Contains metadata about the citation counts process for a specific university. |
| `<university_name>/citation_counts_countries_incoming.csv` | Contains the incoming citation counts for countries for a specific university. |
| `<university_name>/citation_counts_countries_outgoing.csv` | Contains the outgoing citation counts for countries for a specific university. |
| `<university_name>/citation_counts_organizations_incoming.csv` | Contains the incoming citation counts for organizations for a specific university. |
| `<university_name>/citation_counts_organizations_outgoing.csv` | Contains the outgoing citation counts for organizations for a specific university. |
