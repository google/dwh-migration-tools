# Python Exemplary Client

This is an exemplary command-line tool to simplify the process of running a
Batch SQL Translation job using the Google Cloud Bigquery Migration Python
client package.

## Supported Python Versions 
Python >= 3.7

## Installation

Clone or download this repository and install the exemplary Python client: 

```
pip3 install dwh-migration-tools/client
```

Install the gcloud CLI following the [instructions](http://cloud.google.com/sdk/docs/install).

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

## Run a translation job using example inputs.

Simply run the following commands in Python3 to start a translation using the sample query files in [input](input).

```
bin/dwh-migration-client
```
## input and output directory

The input folder is supposed to contain files with pure SQL statements (comments
are OK). The file extension can be in any format like .txt or .sql.

Every input SQL file will have a corresponding output file under the same name in
the output directory.

The default value of input dir is `client/input`. To override it, add the flag `--input path/to/input_dir` when running 
the above command.  

The default value of output dir that stores the outputs of a translation job is `client/output`. To override it, add the 
flag `--output path/to/output_dir` when running the above command.

Example command of overriding the default input/output directory.
```
bin/dwh-migration-client --input path/to/input_dir --output path/to/output_dir
```

### [Optional] macros replacement mapping

This tool can also perform macros substitution before/after the translation job
through an option flag.

To enable macros substitution, pass the arg '-m client/macros.yaml' when
running the tool:

```
bin/dwh-migration-client -m client/macros.yaml
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
bin/dwh-migration-client -o client/object_mapping.json
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

### List of options:

```commandline
options:
  -h, --help            show this help message and exit.
  
      --config path/to/config.yaml             
                        Path to the config.yaml file. (default: client/config.yaml)
    
      --input path/to/input_dir
                        Path to the input_directory. (default: client/input)
  
      --output path/to/output_dir
                        Path to the output_directory. (default: client/output)
  
  -m, --macros path/to/macros.yaml
                        Path to the macro map yaml file. If specified, the program will 
                        pre-process all the input query files by replacing the macros 
                        with corresponding string values according to the macro map
                        definition. After translation, the tool will revert the 
                        substitutions for all the output query files in a 
                        post-processing step. The replacement does not apply for files 
                        with extension of .zip, .csv, .json.
                        
  -o --object_name_mapping path/to/object_mapping.json
                        Path to the object name mapping json file. Name mapping lets 
                        you identify the names of SQL objects in your source files, and 
                        specify target names for those objects in BigQuery. More info
                        please see https://cloud.google.com/bigquery/docs/output-name-mapping.
```

## FAQ:
### `pip install` couldn't find the right version for `google-cloud-storage`?

Make sure you are using Python 3.7 or later to install [google-cloud-storage](https://pypi.org/project/google-cloud-storage/). 

### I am using Python 3.7 or a later version, but I am still seeing errors about packages like `bigquery_migration_v2`.

It's recommended to run this tool in the Python [virtual env](https://docs.python.org/3/library/venv.html) to avoid 
dependency noises. 

Create a Python3 virtual env by `python3 -m venv my_env`, and activate it by `source my_env/bin/activate`.
