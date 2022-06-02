# Install 

Install the client into a virtualenv in editable mode along with its 
dependencies (including dev dependencies).

```shell
git clone git@github.com:google/dwh-migration-tools.git
cd client
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r requirements_dev.txt
pip install -e .[dev]   
```

# Adding and updating dependencies

Add 
[abstract dependencies](https://pipenv.pypa.io/en/latest/advanced/#pipfile-vs-setup-py)
to `setup.py` and then run:

```shell
make requirements_dev
```

# Before committing

```shell
make format
make lint
make check
```