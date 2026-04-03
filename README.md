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

## Contributing

Open a PR with changes. Tests should pass and code should be formatted with
your preferred tools.