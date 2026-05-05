# 1a. Map of Italian Science

1. Prepare the virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Prepare a storage location with enough space (~150GB) it and set the
`STORAGE_PATH` variable in the `.env` file to point to it:

```bash
echo "STORAGE_PATH=/path/to/your/storage" > .env
```

3. Download and extract the IRIS dataset from Zenodo:

```bash
curl -L -o iris.zip "https://zenodo.org/records/18202530/files/data.zip?download=1"
unzip iris.zip -d iris
rm iris.zip
```

4. Download the OpenCitations Meta CSV dump from Zenodo:

```bash
curl -L -o oc_csv.tar.gz "https://zenodo.org/records/18324537/files/output_csv_2026_01_14.tar.gz?download=1"
mkdir -p oc_csv
tar -xzf oc_csv.tar.gz -C oc_csv
rm oc_csv.tar.gz
```

5. Build the SQLite index from the OpenCitations CSV dump:

```bash
python src/oc_index.py
```

6. Run the IRIS pids mapping script to generate the output CSV files for each university:

```bash
python src/iris_pids.py
```
