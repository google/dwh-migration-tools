# Contributing to Batch SQL Translation Client

## Install

Install the client dependencies (including dev dependencies) in a 
virtualenv.

```shell
git clone git@github.com:google/dwh-migration-tools.git
cd dwh-migration-tools
python3 -m venv venv
source venv/bin/activate
pip install -r client/requirements.txt
pip install -r client/requirements_dev.txt
```

## Before committing

```shell
cd client && make check
```