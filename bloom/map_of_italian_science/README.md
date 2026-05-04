# 1a. Map of Italian Science

Download and extract the IRIS dataset from Zenodo:

```bash
curl -L -o data.zip "https://zenodo.org/records/18202530/files/data.zip?download=1"
unzip data.zip -d data
rm data.zip
```

Download the OpenCitations Meta CSV dump from Zenodo:

```bash
curl -L -o oc_csv.tar.gz "https://zenodo.org/records/18324537/files/output_csv_2026_01_14.tar.gz?download=1"
tar -xzf oc_csv.tar.gz -C oc_csv
rm oc_csv.tar.gz
```

Generate an auth token for OpenCitation at https://opencitations.net/accesstoken/,
and add it in a `.env` file:

```bash
cp .env-example .env
nano .env  # add your token in the OPENCITATIONS_TOKEN variable
```

Prepare the virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Launch the script:

```bash
python src/1a.py
```
