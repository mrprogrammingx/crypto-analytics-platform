## Convenience targets for local development

.PHONY: dbt-run
dbt-run:
	@bash scripts/run_dbt.sh

.PHONY: dbt-test
dbt-test:
	@bash scripts/run_dbt.sh test
