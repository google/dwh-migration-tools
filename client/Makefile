SHELL := /bin/bash -ex

.PHONY: requirements
requirements:
	rm -rf /tmp/dwh-migration-client-venv \
		&& python3 -m venv /tmp/dwh-migration-client-venv \
		&& source /tmp/dwh-migration-client-venv/bin/activate \
		&& pip install . \
		&& pip freeze | sort > requirements.txt \
		&& sed -i '/^dwh-migration-client/d' requirements.txt

.PHONY: requirements_dev
requirements_dev: requirements
	rm -rf /tmp/dwh-migration-client-venv \
		&& python3 -m venv /tmp/dwh-migration-client-venv \
		&& source /tmp/dwh-migration-client-venv/bin/activate \
		&& pip install .[dev] \
		&& bash -c 'comm -23 <(pip freeze | sort) requirements.txt' > requirements_dev.txt \
        && sed -i '/^dwh-migration-client/d' requirements_dev.txt

.PHONY: format
format:
	black dwh_migration_client
	isort dwh_migration_client

.PHONY: lint
lint: format
	pylint dwh_migration_client

.PHONY: check
check:
	mypy --strict dwh_migration_client