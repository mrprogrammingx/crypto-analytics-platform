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

