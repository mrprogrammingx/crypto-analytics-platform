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

## Contributing

Open a PR with changes. Tests should pass and code should be formatted with
your preferred tools.
# de-streaming-pipeline