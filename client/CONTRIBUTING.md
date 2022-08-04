# Contributing to Batch SQL Translation Client

## Install

Install the client dependencies (including dev dependencies) in a 
virtualenv.

```shell
git clone git@github.com:google/dwh-migration-tools.git
cd dwh-migration-tools
python3 -m venv venv
source venv/bin/activate
pip install -e client[dev]
```

# Adding dependencies

Add
[abstract dependencies](https://pipenv.pypa.io/en/latest/advanced/#pipfile-vs-setup-py)
to `setup.py`.

## Before committing

```shell
cd client && make check && make test
```