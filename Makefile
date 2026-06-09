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

