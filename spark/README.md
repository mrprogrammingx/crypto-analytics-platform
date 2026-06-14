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
│   └── verify_schema_inference.py
└── README.md
```

Quick overview
- `jobs/read_btc_parquet.py`: ad-hoc job to read Parquet (local or GCS) and
  normalize timestamps.
- `jobs/verify_schema_inference.py`: small helper to compare schema inference
  when reading Parquet with and without `mergeSchema` (useful when partitions
  have differing column types).
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
