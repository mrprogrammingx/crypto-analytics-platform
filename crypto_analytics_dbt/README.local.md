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

Manual invocation
-----------------

If you ever need to run dbt manually (for debugging or CI-less environments) the full command we use under the hood is shown below. You can copy/paste it and change the dbt subcommand (for example `docs generate`, `run` or `test`) as needed.

Explanation: the command does three things:

- `set -a && source ../.env && set +a` — load all variables from the repo `.env` and export them to the environment for the following command.
- `.venv/bin/dbt` — run the dbt binary from the project's virtualenv (preferred so versions match the repo).
- `--profiles-dir .` — tell dbt to load `profiles.yml` from the project directory instead of the default `~/.dbt` (useful for local development).

Examples (zsh / bash):

Generate docs:

```
set -a && source ../.env && set +a && .venv/bin/dbt docs generate --profiles-dir .
```

Run models:

```
set -a && source ../.env && set +a && .venv/bin/dbt run --profiles-dir .
```

Run tests:

```
set -a && source ../.env && set +a && .venv/bin/dbt test --profiles-dir .
```

Notes:

- You don't normally need this long command because the repository provides `make dbt-run`, `make dbt-test` and `scripts/run_dbt.sh` which automatically source `.env` and prefer the repo `.venv`.
- If you keep your credentials in another location, change the `source ../.env` part to point to your credentials file or use `GOOGLE_APPLICATION_CREDENTIALS` instead.
- Avoid committing secrets; keep `.env` in `.gitignore`.
