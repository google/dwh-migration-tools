SHELL := /bin/bash -ex

.PHONY: format
format:
	black .
	isort .

.PHONY: lint
lint:
	pylint *.py

.PHONY: check
check: format lint
