# Developer entry points.
#
# The one thing worth knowing before reading further: this project needs TWO
# Python environments, and that is not an accident of setup.
#
#   global      databricks-connect, the Databricks CLI, dbt. Used for talking
#               to a real workspace.
#   .venv-test  real pyspark. Used for running transformations locally.
#
# They cannot be merged. `databricks-connect` installs itself *as* the pyspark
# package, and its SparkSession refuses to start locally:
#
#     RuntimeError: Only remote Spark sessions using Databricks Connect are
#     supported.
#
# So integration tests get their own interpreter. `make test` skips them
# automatically on the global one; `make test-integration` runs them properly.

VENV_TEST := .venv-test
PY_TEST   := $(VENV_TEST)/Scripts/python.exe
PY        := python

.DEFAULT_GOAL := help
.PHONY: help venv-test test test-integration test-all lint fmt hooks clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

$(PY_TEST):
	@echo ">> Creating $(VENV_TEST) with real pyspark (a few minutes, once)"
	$(PY) -m venv $(VENV_TEST)
	$(PY_TEST) -m pip install --quiet --upgrade pip
	$(PY_TEST) -m pip install --quiet -r requirements-test.txt

venv-test: $(PY_TEST)  ## Create the test virtualenv

test:  ## Unit + contract tests (fast, no Spark)
	$(PY) -m pytest tests/ -q

test-integration: $(PY_TEST)  ## Integration tests against real local Spark
	$(PY_TEST) -m pytest tests/integration -q

test-all: test test-integration  ## Everything

lint:  ## Lint
	ruff check src/ tests/

fmt:  ## Auto-fix what ruff can
	ruff check --fix src/ tests/

hooks:  ## Install pre-commit hooks
	pre-commit install
	@echo ">> Hooks installed. Run 'pre-commit run --all-files' for a first pass."

clean:  ## Remove caches (keeps .venv-test)
	rm -rf .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -not -path './.venv-test/*' -exec rm -rf {} + 2>/dev/null || true
