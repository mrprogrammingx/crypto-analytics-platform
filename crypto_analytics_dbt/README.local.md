Local dbt run (convenience)
---------------------------

This project includes a small helper script and Makefile target to run dbt while automatically sourcing the repository `.env` file.

Usage

- Create a local `.env` in the repository root with your credentials and settings (this repo already has one).
- Ensure the project's virtualenv is created at `crypto_analytics_dbt/.venv` (the repository includes commands to create it).

Run with:

```
make dbt-run
```

Or directly:

```
bash scripts/run_dbt.sh
```

The script will prefer `crypto_analytics_dbt/.venv/bin/dbt` if present, otherwise it falls back to a system `dbt` on PATH.
