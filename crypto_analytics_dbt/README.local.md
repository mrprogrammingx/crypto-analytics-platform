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

profiles.yml example
--------------------

This repo includes `profiles.yml.example` which shows the expected dbt profile for this project. To use it locally:

- Copy it to your user dbt profile location (recommended):

```
cp crypto_analytics_dbt/profiles.yml.example ~/.dbt/profiles.yml
```

- Or copy into the project and edit if you prefer:

```
cp crypto_analytics_dbt/profiles.yml.example crypto_analytics_dbt/profiles.yml
```

If you create a local `profiles.yml`, consider adding it to your global or project `.gitignore` so secrets are not committed.

Running tests
-------------

You can run dbt's tests (data tests and schema tests) locally without manually sourcing `.env` by using the helper script or Makefile target.

- Recommended (shortcut):

```
make dbt-test
```

- Using the wrapper directly:

```
bash scripts/run_dbt.sh test
```

- If you prefer to run dbt directly (not recommended for daily use):

```
set -a && source ../.env && set +a && .venv/bin/dbt test --profiles-dir .
```

The Makefile/wrapper will automatically source the repository `.env` and prefer the project's virtualenv dbt binary.
