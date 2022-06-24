SHELL := /bin/bash -ex

.PHONY: format
format:
	black .
	isort .

.PHONY: lint
lint:
	pylint *.py

.PHONY: typecheck
typecheck:
	mypy --strict .

.PHONY: check
check: format lint typecheck
