## Convenience targets for local development

.PHONY: dbt-run
dbt-run:
	@bash scripts/run_dbt.sh

.PHONY: dbt-test
dbt-test:
	@bash scripts/run_dbt.sh test

.PHONY: dbt-docs-generate
dbt-docs-generate:
	@bash scripts/run_dbt.sh docs generate

.PHONY: dbt-docs-serve
dbt-docs-serve:
	@bash scripts/run_dbt.sh docs serve

.PHONY: dbt-docs
dbt-docs: dbt-docs-generate dbt-docs-serve

.PHONY: export-lineage
export-lineage: dbt-docs-generate
	@echo "Exporting lineage PNGs (spring + dot)"
	@./crypto_analytics_dbt/.venv/bin/python scripts/export_lineage_png.py --out assets/lineage_graph_spring.png --layout spring --width 10 --height 6 --dpi 120 --k 0.3
	@./crypto_analytics_dbt/.venv/bin/python scripts/export_lineage_png.py --out assets/lineage_graph_dot.png --layout dot --width 12 --height 9 --dpi 130 --prog dot

.PHONY: dbt-run-copy
dbt-run-copy:
	@bash scripts/run_dbt_with_copy.sh

.PHONY: dbt-test-copy
dbt-test-copy:
	@bash scripts/run_dbt_with_copy.sh test


.PHONY: spark-test
spark-test:
	@echo "Setting up venv (./venv) and running Spark tests"
	@python3 -m venv venv || true
	@. venv/bin/activate && python -m pip install --upgrade pip setuptools wheel && \
	if [ -f requirements-dev.txt ]; then . venv/bin/activate && pip install -r requirements-dev.txt || true; fi && \
	. venv/bin/activate && pip install pyarrow pytest || true && \
	. venv/bin/activate && PYTHONPATH=$$PWD pytest -q tests/test_read_btc_parquet.py -q

.PHONY: spark-ingest-bronze
spark-ingest-bronze:
	@. venv/bin/activate && PYTHONPATH=$$PWD python spark/jobs/ingest_bronze.py --source gcs