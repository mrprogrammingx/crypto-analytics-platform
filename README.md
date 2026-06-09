# crypto-analytics-platform

Lightweight repo for fetching crypto data and small tooling around external
APIs. This README covers local setup, running the example Binance websocket
client, how to run the tests, and how to configure environment variables.

## Quickstart

1. Create a Python virtual environment and activate it:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Upgrade packaging tools and install runtime deps:

```bash
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

3. Configure environment variables (optional):

Copy the example and fill values if you need exchange API keys (Binance):

```bash
cp .env.example .env
# then edit .env and set BINANCE_API_KEY and BINANCE_API_SECRET if needed
```

4. Run the Binance example (opens a websocket to Binance trade stream):

```bash
# activate venv first (if not already active)
source .venv/bin/activate
python -m ingestion.binance_websocket
```

Note: `ingestion/binance_websocket.py` requires `websocket-client` (included in `requirements.txt`).

## Tests

Run the test suite with pytest:

```bash
pip install pytest
pytest -q
```

The tests include a small HTTP server (used previously) and a unit test to
verify the `ingestion` package import.

## Configuration

Environment variables are loaded from `.env` by the helper in `config.py`.
See `.env.example` for the supported variables. Key ones:

- `BINANCE_API_KEY` — your Binance API key (optional)
- `BINANCE_API_SECRET` — Binance secret (optional)

Use `from config import load_config; cfg = load_config()` in your code to access typed settings.

## Development notes

Utilities for configuration and examples live in `config.py` and the
`ingestion` package.

- `ingestion/binance_websocket.py` is a small example connector that opens a
	WebSocket to Binance and prints trade messages. It exposes a
	`run_binance_socket()` function and an `on_message()` handler.

Local dbt development
---------------------

This repository includes a dbt project for transformations located in
`crypto_analytics_dbt`. For local development the project provides a
convenience README with instructions, a wrapper script (`scripts/run_dbt.sh`)
that sources the repository `.env`, and Makefile targets (`make dbt-run`,
`make dbt-test`, `make dbt-docs`) which prefer the project's `.venv`.

See `crypto_analytics_dbt/README.local.md` for full details and examples.

## Kafka integration

This repository now includes a small Kafka-based ingestion example (producer
and consumer) and a `docker-compose.yml` to run a local Zookeeper + Kafka
broker for development.

Quick steps to run locally:

1. Start Kafka & Zookeeper in the background (uses the included
	`docker-compose.yml`):

```bash
docker-compose up -d
```

2. Verify Kafka is running and list topics:

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

3. Install the Python dependency for Kafka (if not already installed):

```bash
source .venv/bin/activate
pip install kafka-python
```

4. Run the producer (runs the Binance websocket and sends formatted trade
	events to topic `btc_trades`). Run it as a module so package imports work:

```bash
# from the repository root, with venv activated
python -m ingestion.kafka_producer
```

5. Run the consumer to read messages from `btc_trades`:

```bash
python -m ingestion.kafka_consumer
```

Notes and tips
- The producer/consumer use `kafka-python`. Consider adding `kafka-python` to
  `requirements.txt` so others can install all runtime deps with `pip install -r requirements.txt`.
- Avoid running the scripts directly (e.g. `python ingestion/kafka_producer.py`) — run them via `-m` to ensure package-relative imports resolve.
- If you don't want to run Kafka locally, you can run `python -m ingestion.binance_websocket` to print trade messages to stdout for testing.
- Tests that exercise Kafka may produce verbose Docker logs; run unit tests individually to limit noise (e.g. `pytest tests/test_ingestion.py`).

## BigQuery helpers

The repository contains a small helper script to create a BigQuery dataset and
table used by the ingestion pipeline:

- `scripts/create_bigquery_table.py` — creates the dataset (if missing) and a
	`btc_trades` table with a simple schema (symbol, price, quantity, timestamp).

Usage:

```bash
# from the repository root
PYTHONPATH=. .venv/bin/python scripts/create_bigquery_table.py
```

The script reads `GOOGLE_CLOUD_PROJECT` and `BIGQUERY_TABLE_ID` from your `.env` (or
environment). It's safe to run repeatedly — it will only create resources when
they're missing.

BigQuery loader
---------------

The repository includes a small BigQuery loader that moves Parquet files stored
in GCS into a BigQuery table used by the ingestion pipeline.

- `loaders/bigquery_loader.py` — loads one or more Parquet files from GCS into
	BigQuery. It expects the project/dataset/table configuration to be available
	via the project's config loader (see `.env.example`).

Key behaviors and env vars

- Idempotency: the loader records which Parquet object URIs have been loaded in
	a tracking table (configurable via `BIGQUERY_TRACKING_TABLE`). On subsequent
	runs it filters out files that were already recorded so you don't accidentally
	reload the same file twice.
- Wildcard expansion: if you provide a GCS prefix or wildcard, the loader will
	pre-list matching objects and expand the wildcard to an explicit list of
	`gs://...` URIs before submitting the BigQuery load job (this avoids
	NotFound/NotAuthorized errors when passing wildcards directly).
- Important env vars: `GOOGLE_CLOUD_PROJECT`, `GOOGLE_APPLICATION_CREDENTIALS`
	(or ADC), and the centralized table names `BIGQUERY_DATASET`,
	`BIGQUERY_TABLE_BTC_TRADES`, and `BIGQUERY_TRACKING_TABLE` (see `.env.example`).

Creating the tracking table

The loader expects the tracking table to exist. You can create it with the
helper script included in this repo:

```bash
# from the repository root
PYTHONPATH=. .venv/bin/python scripts/create_bigquery_table.py --tracking
```

Running the loader

Quick test (safe): limit your GCS prefix to a small set of objects and run:

```bash
# from the repository root (with venv activated)
PYTHONPATH=. .venv/bin/python loaders/bigquery_loader.py
```

Notes & recommendations

- The loader implements file-level idempotency by tracking processed object
	URIs. For production workloads that may reprocess data frequently, consider
	adding row-level deduplication (e.g., load to a staging table and `MERGE`
	into the final table) to guarantee no duplicate rows at the record level.
- If you run into permission errors, ensure your ADC or `GOOGLE_APPLICATION_CREDENTIALS`
	is configured and that the service account has `roles/storage.objectViewer`
	and the BigQuery load permissions for the target dataset/table.

DuckDB loader
---------------

This project also includes a small DuckDB loader that can ingest the same
Parquet files from GCS into a local DuckDB file for fast local analysis.

- `loaders/duckdb_loader.py` — downloads Parquet objects from GCS and inserts
	them into DuckDB tables (it also records which files were already loaded so
	reruns are idempotent).

Key environment variables (see `.env.example`):

- `DUCKDB_DATABASE` — path to the DuckDB file (default: `analytics.duckdb`).
- `DUCKDB_LIMIT` — optional integer to limit the number of files processed (useful for dry-runs and testing).
- `BIGQUERY_DATASET`, `BIGQUERY_TABLE_BTC_TRADES`, `BIGQUERY_TRACKING_TABLE` — centralized table naming used by both BigQuery and DuckDB loaders.

Notes about behavior:

- The loader downloads each Parquet file using the `google-cloud-storage` client
	(uses your Application Default Credentials) and then reads the local Parquet
	file with DuckDB. This avoids configuring DuckDB HTTPFS credentials and is
	robust across environments.
- Some Parquet files store timestamps as integer epoch milliseconds. The loader
	detects this and automatically retries the insert using a conversion
	(to_timestamp(timestamp/1000)).

Running the DuckDB loader (dry-run/test):

```bash
# limit to 5 files for a safe test
DUCKDB_LIMIT=5 PYTHONPATH=. .venv/bin/python loaders/duckdb_loader.py
```

To run the full load (may download many files):

```bash
PYTHONPATH=. .venv/bin/python loaders/duckdb_loader.py
```

If you prefer DuckDB to read directly from GCS (no intermediate download) you
can configure DuckDB HTTPFS credentials; otherwise the download-based loader
is the simplest, reliable approach.

Architecture
------------

This repository follows a simple analytics architecture designed for small to
medium-sized data projects. The key goals are: simplicity, auditability, and
repeatable transformations using dbt.

Medallion Architecture
----------------------

We use a medallion (bronze/silver/gold) pattern for staged transformations:

- Bronze: raw or lightly-transformed data from ingestion. In this repo the
	`bronze` models materialize the initial cleaned records from Parquet objects
	produced by the ingestion pipeline.
- Silver: intermediate, canonical models that standardize types, perform
	basic aggregations and quality checks (`models/silver`).
- Gold: high-level business views and aggregated summaries for analytics and
	reporting (`models/gold`).

This structure keeps transformations modular and easier to test and reason
about.

dbt Transformations
-------------------

Transformations live in the dbt project at `crypto_analytics_dbt`. The dbt
project contains `models/staging`, `models/bronze`, `models/silver`, and
`models/gold` folders which correspond to the medallion layers above.

Useful commands (from the repository root):

```bash
make dbt-run        # run models
make dbt-test       # run schema & data tests
make dbt-docs       # generate and serve docs locally
```

Lineage Graph
-------------

dbt automatically builds a model dependency graph. The UI exposed by
`dbt docs serve` shows:

- the lineage graph (downstream/upstream dependencies),
- dependency tracking between models, and
- model and column documentation (including schema tests).

This is a great visual asset to include in a portfolio or to show during
interviews — it clearly demonstrates the end-to-end transformation flow.

Lineage PNGs
------------

Two exported lineage images are included in `assets/` for quick viewing:

- `assets/lineage_graph_spring.png` — compact force-directed layout (spring layout) suitable for smaller thumbnails.
- `assets/lineage_graph_dot.png` — graphviz 'dot' layout which emphasizes hierarchical dependencies.

You can regenerate them locally with:

```bash
make export-lineage
```

Analytics Engineering
---------------------

Some notes on engineering practices used in this repo:

- Modular dbt models: small, single-purpose models that are easy to test and
	document.
- Tests: schema and data tests live alongside models so quality checks run
	with `dbt test`.
- Idempotent loaders: BigQuery and DuckDB loaders record processed files in a
	tracking table to avoid double-loading.
- Local-first workflow: wrapper scripts and Makefile targets make it easy to
	run dbt locally (recommended for development). Consider adding CI to run
	tests and docs on push.

CI and running dbt on a server
-------------------------------

Some dbt commands (notably `dbt test` against BigQuery and `dbt docs generate`)
require access to Google Cloud credentials and the target BigQuery dataset. For
this reason the Makefile/wrapper targets may not work out-of-the-box on a
developer machine unless you have ADC or `GOOGLE_APPLICATION_CREDENTIALS` and
the dataset configured locally.

To provide a reproducible server-run of tests and docs this repository includes
a GitHub Actions workflow: `.github/workflows/dbt-ci.yml`.

What the workflow does:

- Runs Python unit tests (`pytest`).
- Installs `dbt-core` and the `dbt-bigquery` adapter and runs `dbt test`.
- Generates the dbt docs (`dbt docs generate`) and uploads the generated
	`crypto_analytics_dbt/target` directory as a workflow artifact.
- On `main` the docs artifact is published to GitHub Pages (so docs can be
	hosted publicly if you enable Pages for this repo).

To run dbt tests/docs on CI you should add the following repository secrets
in GitHub (Repository -> Settings -> Secrets):

- `GCP_SA_KEY` — base64 or JSON string of a Google service account key with
	permission to run BigQuery jobs and access the dataset.
- `GOOGLE_CLOUD_PROJECT` and `BIGQUERY_DATASET` — values referenced by the
	dbt `profiles.yml` in `crypto_analytics_dbt`.

If you want, I can also add a helper workflow that only builds and uploads
the docs (no tests) or a small script to export the lineage graph PNG and
commit it to the repo for portfolio use.


## Contributing

Open a PR with changes. Tests should pass and code should be formatted with
your preferred tools.