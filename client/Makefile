
SHELL := /bin/bash -ex

.PHONY: requirements
requirements:
	$(eval TMP := $(shell mktemp -d))
	python3 -m venv $(TMP) \
		&& source $(TMP)/bin/activate \
		&& pip install . \
		&& pip freeze | sort > requirements.txt \
		&& sed -i '/^dwh-migration-client/d' requirements.txt
	rm -rf $(TMP)
	$(eval TMP := $(shell mktemp -d))
	python3 -m venv $(TMP) \
		&& source $(TMP)/bin/activate \
		&& pip install .[dev] \
		&& bash -c 'comm -23 <(pip freeze | sort) requirements.txt' > requirements_dev.txt \
		&& sed -i '/^dwh-migration-client/d' requirements_dev.txt
	rm -rf $(TMP)

.PHONY: format
format:
	black dwh_migration_client
	isort dwh_migration_client

.PHONY: lint
lint:
	pylint dwh_migration_client

.PHONY: typecheck
typecheck:
	mypy --strict dwh_migration_client

.PHONY: check
check: format lint typecheck
