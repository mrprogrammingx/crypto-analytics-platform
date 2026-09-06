# Spark Development

This folder contains utilities, a small job and a development notebook to
work with Parquet produced by the ingestion pipeline.

Structure
```
spark/
├── notebooks/
│   └── spark_exploration.ipynb
├── jobs/
│   ├── read_btc_parquet.py
│   ├── verify_schema_inference.py
│   ├── ingest_bronze.py
│   ├── schema_utils.py
│   └── gcs_sync.py
└── README.md
```

Quick overview
- `jobs/read_btc_parquet.py`: ad-hoc job to read Parquet (local or GCS) and
  normalize timestamps.
- `jobs/verify_schema_inference.py`: small helper to compare schema inference
  when reading Parquet with and without `mergeSchema` (useful when partitions
  have differing column types).
- `jobs/ingest_bronze.py`: the Bronze ingestion job — reads raw parquet/csv,
  normalizes timestamps, derives a date partition, and writes a partitioned
  Bronze Parquet dataset. See "Bronze ingestion pipeline" below.
- `jobs/schema_utils.py`: shared mergeSchema-conflict-with-fallback logic used
  by `ingest_bronze.py` (extracted from the `verify_schema_inference.py`
  approach so the ingestion job gets the same schema-drift handling).
- `jobs/gcs_sync.py`: plain-Python (no JVM) helpers for listing/downloading
  new raw files from GCS and uploading Bronze output back, used by
  `ingest_bronze.py --source gcs`.
- `notebooks/spark_exploration.ipynb`: a developer notebook for iterative
  exploration and debugging.

Verify schema inference
-----------------------

When Parquet files are produced by multiple writers or with evolving schemas,
Spark's schema inference can produce different results depending on options
like `mergeSchema` or whether partitions are read separately.

To compare behavior locally:

```bash
# run with venv activated (see top-level README)
python spark/jobs/verify_schema_inference.py --paths /path/to/part-dir/*.parquet
```

This script will read the provided Parquet files twice: once with
`mergeSchema=false` and once with `mergeSchema=true`, printing both schemas
and a simple diff so you can reason about any mismatches.

Bronze ingestion pipeline
-------------------------

`jobs/ingest_bronze.py` is the Bronze stage of the pipeline: it turns the raw
Parquet that `ingestion/kafka_consumer_parquet_gcs.py` writes to
`gs://<bucket>/btc_trades/year=/month=/day=/*.parquet` into a clean,
partitioned, deduplicated Bronze Parquet dataset that dbt's `stg_btc_prices`
model reads from (via `BIGQUERY_TABLE_ID`).

It runs in two modes:

- `--source local` (default): reads explicit `--inputs` paths/globs and
  `--output`, exactly as before — this is the original ad-hoc/dev usage and
  every existing local invocation keeps working unchanged.
- `--source gcs`: the batch-pipeline mode. It lists new raw files in
  `gs://<bucket>/<SPARK_RAW_GCS_PREFIX>/` (default `btc_trades/`), downloads
  only files added since the last run, runs the same
  normalize/dedupe/quality-filter logic, appends the result to
  `SPARK_BRONZE_LOCAL_PATH` (default `warehouse/bronze/btc_trades`), and
  uploads that output to `gs://<bucket>/<SPARK_BRONZE_GCS_PREFIX>/`.

```bash
# manual / dev run
make spark-ingest-bronze

# equivalent direct invocation
PYTHONPATH=$PWD python spark/jobs/ingest_bronze.py --source gcs
```

**Idempotency (watermark).** GitHub Actions runners are ephemeral, so
tracking state can't live on local disk between scheduled runs. Instead,
`gcs_sync.py` stores a single small watermark object in GCS at
`gs://<bucket>/bronze/_state/watermark.json` (`{"last_processed_at": ...}`)
and only pulls raw blobs newer than it. A run with no new raw files since the
watermark prints `No new raw files since last watermark; nothing to do.` and
exits without touching Bronze output — safe to run as often as you like
(e.g. on a schedule) without reprocessing or duplicating data.

**Scheduled runs.** `.github/workflows/spark-bronze-ingest.yml` runs
`ingest_bronze.py --source gcs` hourly (and on `workflow_dispatch`) against
the real bucket using `GCP_SA_KEY`/`GOOGLE_CLOUD_PROJECT`/`GCS_BUCKET_NAME`
secrets, then refreshes the BigQuery external table (see below) when
credentials are present.

**Exposing Bronze output as `btc_trades`.** `stg_btc_prices.sql` is
unchanged — it still selects `from {{ env_var('BIGQUERY_TABLE_ID') }}`. Only
what backs that name changes:

- DuckDB target: `python scripts/register_duckdb_bronze_view.py` creates/
  replaces a view over the local Bronze Parquet output.
- BigQuery target: `python scripts/create_bigquery_external_table.py`
  creates/replaces a BigQuery **external table** over the GCS Bronze output
  (hive-partitioned on `date`), so BigQuery always reflects whatever Spark
  last uploaded with no separate load step.

`loaders/bigquery_loader.py` and `loaders/duckdb_loader.py` are superseded by
this pipeline and kept only as a manual fallback (not wired into any script
or CI job here).

**Naming note.** dbt has its own model called `bronze_btc_prices`
(`crypto_analytics_dbt/models/bronze/`), which is an unrelated SQL cleaning
step over `stg_btc_prices`. "Bronze" here refers to this Spark Parquet
landing zone, not that dbt model — the two are independent layers in
different engines that happen to share a medallion-architecture name.

Architecture changes (notes)
---------------------------

We added a small Spark surface for local dev and CI validation:

- Lightweight PySpark job for local inspection (`read_btc_parquet.py`).
- Developer notebook with examples for reading Parquet and checking types.
- A CI job (`spark-tests`) that installs Java and runs Spark-only tests in an
  isolated runner so main CI jobs remain fast.

These changes keep heavy Spark/JVM dependencies isolated to a separate dev
path and CI job, while keeping the primary CI pipeline fast for dbt and pytest.

How to run the notebook
-----------------------

Open `spark/notebooks/spark_exploration.ipynb` in Jupyter or VS Code's
notebook support. The notebook includes cells that build a local Spark
session and show schema inference examples.
