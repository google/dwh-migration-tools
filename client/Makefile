
SHELL := /bin/bash -ex

.PHONY: format
format:
	black dwh_migration_client tests noxfile.py
	isort dwh_migration_client tests noxfile.py

.PHONY: lint
lint:
	pylint dwh_migration_client noxfile.py
	find tests -type f -name "*.py" | xargs pylint

.PHONY: typecheck
typecheck:
	mypy --strict dwh_migration_client

.PHONY: check
check: format lint typecheck

.PHONY: test
test:
	nox --sessions test
