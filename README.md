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


## Contributing

Open a PR with changes. Tests should pass and code should be formatted with
your preferred tools.