# Python Exemplary Client

This is an exemplary command-line tool to simplify the process of running a
Batch SQL Translation job using the Google Cloud Bigquery Migration Python
client package.

## Installation

Install the gcloud CLI following the [instructions](http://cloud.google.com/sdk/docs/install).

Clone or download this repository, create a virtualenv, activate it and install
the exemplary Python client:

```shell
git clone git@github.com:google/dwh-migration-tools.git
cd client
python3 -m venv venv
source venv/bin/activate
pip3 install .
```

### [Optional] Install in editable mode

If you plan on editing the Python files of the client directly, you will want to
install it in editable mode using `-e`:

```shell
pip3 install -e .
```

### [Optional] Install the recommended concrete dependencies

Running `pip3 install .` will install concrete dependencies from a list of 
abstract dependencies defined in `setup.py`. If you want to ensure your 
installation is using the same concrete dependencies that the exemplary client
was developed against, then run the following before running either of the
commands above:

```shell
pip3 install -r requirements.txt
```

### [Optional] gcloud login and authentication

The program will first validate the login and credential status of
gcloud. If the validation steps failed, the program will run the following two
commands automatically and bring up a page on browser for your agreement of
using your Google account.

However, it's recommended that you run these two commands on the terminal first as a one-time setup requirement:

Log in to gcloud:

```
gcloud auth login
```

Generate an application-default credential file so that you can use gcloud API
programmatically:

```
gcloud auth application-default login
```

## User Manual

Open the [config.yaml](config.yaml) file and fill all the required fields. If you are a first
time user who just wants to give it a try, we recommend to create a new [GCP
project](https://console.cloud.google.com/) and put the project_number (or project_id) in the `project_number` field in 
the config.

If you want to use an existing project, make sure you have all the required [IAM
permissions](https://cloud.google.com/bigquery/docs/batch-sql-translator#required_permissions).

## input_directory

The input folder is supposed to contain files with pure SQL statements (comments
are OK). The file extension can be in any format like .txt or .sql.

## output_directory

In the config, specify a local directory to store the outputs of the translation job. 
Every input SQL file will have a corresponding output file under the same name in 
the output directory.

## Run a translation job

Simply run the following commands in Python3:

```
dwh-migration-client -c config/config.yaml
```

### [Optional] macros replacement mapping

This tool can also perform macros substitution before/after the translation job
through an option flag.

To enable macros substitution, pass the arg '-m macros.yaml' when
running the tool:

```
dwh-migration-client -m config/macros.yaml
```

Here is an example of the macros.yaml file:

```
# This is an example of a macros.yaml file
macros:
  '*.sql':
    '${MACRO_1}': 'macro_replacement_1'
    '%MACRO_2%': 'macro_replacement_2'
  '2.sql':
    'templated_column': 'replacing_column'
```

The tool will perform the following operations on the SQL files:

Before translation starts (the pre-processing step): For every input file ended
with '.sql', the '${MACRO_1}' and '%MACRO_2%' strings will be replaced with
'macro_replacement_1' and 'macro_replacement_2', respectively. For the file
'2.sql', this tool will also replace 'templated_column' with 'replacing_column'.

After translation finishes (the post-processing step): The tool will reverse the
substitution for all the output SQL files by replacing the values with keys in
the map.

Notes that the tool just performs strict string replacement for all the macros
(keys defined in the map) in a single path. During post-processing, a reverse
map is first computed by simply swapping the keys and values for each file in
the map. Unexpected behavior can happen if different macros are mapped to the
same value.


### [Optional] Object name mapping (ONM)

Name mapping lets you identify the names of SQL objects in your source files, and specify target names for those objects
in BigQuery.  For more information about ONM and the format of the json file, please see [here](https://cloud.google.com/bigquery/docs/output-name-mapping#json_file_format). 

To enable object name mapping, pass the optional arg '-o path/to/object_mapping.json' when
running the tool, e.g.:

```
dwh-migration-client -o config/object_mapping.json
```

Here is an example of the object_mapping.json file:

```
{
  "name_map": [{
    "source": {
      "type": "RELATION",
      "database": "my_project",
      "schema": "dataset2",
      "relation": "table2"
    },
    "target": {
      "database": "bq_project",
      "schema": "bq_dataset2",
      "relation": "bq_table2"
    }
  }, {
    "source": {
      "type": "DATABASE",
      "database": "my_project"
    },
    "target": {
      "database": "bq_project"
    }
  }]
}
```

The name mapping rules in this example make the following object name changes:

* Rename instances of the my_project.dataset2.table2 relation object to bq_project.bq_dataset2.bq_table2.
* Renames all instances of the my_project database object to bq_project. For example, my_project.my_dataset.table2 
becomes bq_project.my_dataset.table2, and CREATE DATASET my_project.my_dataset becomes CREATE DATASET 
bq_project.my_dataset.
