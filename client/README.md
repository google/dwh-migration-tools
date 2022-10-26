# BigQuery Migration Service Batch SQL Translator CLI

The [BigQuery Migration Service][BQMS] includes a product known as the
[batch SQL translator][batch SQL translator] that supports translation of a 
wide variety of SQL dialects to Google Standard SQL. This command line tool
simplifies the process of using the batch SQL translator by providing a
framework for pre and postprocessing of SQL files as well as upload/download of
SQL files to/from GCS.

- [Quickstart](#quickstart)
- [Installation](#installation)
- [Basic Usage](#basic-usage)
  - [Environment Variables](#environment-variables)
  - [config.yaml](#configyaml)
  - [Recommended Usage](#recommended-usage)
- [Metadata Lookup](#metadata-lookup)
    - [DDL and Metadata Dump](#ddl-and-metadata-dump)
    - [Unqualified References](#unqualified-references)
- [Pre and Postprocessing](#pre-and-postprocessing)
  - [Handling Macro/Templating Languages](#handling-macrotemplating-languages)
  - [Custom Pre and Postprocessing Logic](#custom-pre-and-postprocessing-logic)
- [Renaming SQL Identifiers](#renaming-sql-identifiers)
- [Deploying to Cloud Run](#deploying-to-cloud-run)

## Quickstart

```shell
# Install the CLI as described in the Installation section below.

# Copy the example project directory to a project directory of your own.
cp -R examples/teradata/sql <YOUR_PROJECT_DIRECTORY>

# Change directory to your project directory.
cd <YOUR_PROJECT_DIRECTORY>

# Remove the example input files from the input directory.
rm -rf input/*

# Copy the files you would like to translate into the input directory.
cp <YOUR_INPUT_FILES> input/

# Edit the config/config.yaml file appropriately as described in the Basic Usage
# section below.
nano config/config.yaml

# Set the appropriate environment variables as described in the Basic Usage
# section below.
export BQMS_VERBOSE="False"
export BQMS_MULTITHREADED="True"
export BQMS_PROJECT="<YOUR_GCP_PROJECT>"
export BQMS_GCS_BUCKET="<YOUR_GCS_BUCKET>"

# Run the translation.
./run.sh
```

## Installation

Prerequisites: Python >= 3.7.2.

Preferred OS: Linux or MacOS.

```shell
pip install .
```

## Basic Usage

There are two ways to run the CLI: directly and via a wrapping  `run.sh` script.

Direct execution requires setting
[environment variables](#environment-variables) and modifying a
[config.yaml](#configyaml) file.

`run.sh` execution is described in detail in 
[Recommended Usage](#recommended-usage).

### Environment Variables

The CLI accepts the following environment variables:

  - `BQMS_PROJECT` **(required)**: The project that the translation job will be
    run in. All GCS paths must reside in this project.
  - `BQMS_INPUT_PATH` **(required)**: The path that raw input to be preprocessed
    will be read from . This can either be a local path or a path that 
    begins with the `gs://` scheme.
  - `BQMS_PREPROCESSED_PATH` **(required)**: The path that the batch SQL
    translator will read preprocessed input from. It must be a path that begins
    with the `gs://` scheme and thus always reside on GCS.
  - `BQMS_TRANSLATED_PATH` **(required)**: The path that the batch SQL
    translator will write translated output to for postprocessing. It must be a
    path that begins with the `gs://` scheme and thus always reside on GCS.
  - `BQMS_POSTPROCESSED_PATH` **(required)**: The path that postprocessed output
    will be written to. This can either be a local path or a path that
    begins with the `gs://` scheme.
  - `BQMS_CONFIG_PATH` **(required)**: The path to the
    [config.yaml](#configyaml) file. This can either be a local path or a path
    that begins with the `gs://` scheme.
  - `BQMS_MACRO_MAPPING_PATH`: The path to the
    [macro mapping](#handling-macrotemplating-languages) file. This can either
    be a local path or a path that begins with the `gs://` scheme.
  - `BQMS_OBJECT_NAME_MAPPING_PATH`: The path to the
    [object name mapping](#renaming-sql-identifiers) file. This can either be a
    local path or a path that begins with the `gs://` scheme.
  - `BQMS_MULTITHREADED`: Set to `True` to enable multithreaded 
    pre/postprocessing and uploads/downloads.
  - `BQMS_VERBOSE`: Set to `True` to enable debug logging.

### config.yaml

The CLI requires a `config.yaml` file be passed as the `BQMS_CONFIG_PATH`
environment variable. This file specifies the translation type (i.e. source and
target dialects), translation location and default values for
[unqualified references](#unqualified-references).

Example:

```yaml
# The type of translation to perform e.g. Teradata to BigQuery. Doc:
# https://cloud.google.com/bigquery/docs/reference/migration/rest/v2/projects.locations.workflows#migrationtask
translation_type: Translation_Teradata2BQ

# The region where the translation job will run.
location: 'us'

# Default database and schemas to use when looking up unqualified references:

# https://cloud.google.com/bigquery/docs/output-name-mapping#default_database
default_database: default_db

# https://cloud.google.com/bigquery/docs/output-name-mapping#default_schema
schema_search_path:
  - library
  - foo
```

### Recommended Usage

The `examples` directory includes a recommended directory structure for using
the CLI:

```shell
> tree examples/teradata/sql
.
├── clean.sh
├── config
│     ├── config.yaml
│     ├── macros_mapping.yaml
│     └── object_name_mapping.json
├── deprovision.sh
├── input
│     ├── case-sensitivity.sql
│     ├── create-procedures.sql
│     ...
│     ├── metadata
│     │     └── compilerworks-teradata-metadata.zip
│     ├── subdir
│     │     ├── test0.sql
│     │     ...
|     ...
├── provision.sh
└── run.sh
```

This directory structure is meant to be used in conjunction with the `run.sh`
script within the directory instead of executing the CLI directly. Doing so
provides the following benefits:

  - Automatic setting of most [environment variables](#environment-variables)
    based on the recommended directory structure. Modifying the
    [config.yaml](#configyaml) file is still required. 
  - Downloading of intermediate pre and postprocessed files from GCS for
    debugging as described in [Pre and Postprocessing](#pre-and-postprocessing). 
  - Execution via Cloud Run as  described in
    [Deploying to Cloud Run](#deploying-to-cloud-run).

The `run.sh` script accepts the following environment variables when
run locally:

  - `BQMS_PROJECT` **(required)**: The project that the translation job will be
    run in.
  - `BQMS_GCS_BUCKET` **(required)**: The GCS bucket where preprocessed and
    translated code will be written to.
  - `BQMS_MULTITHREADED`: Set to `True` to enable multithreaded
    pre/postprocessing and uploads/downloads.
  - `BQMS_VERBOSE`: Set to `True` to enable debug logging.

For a list of environment variables accepted by the `run.sh` script when using
Cloud Run, please see [Deploying to Cloud Run](#deploying-to-cloud-run).

In addition to the `run.sh` script, the example directory also contains the
following shell scripts for convenience:

  - `provision.sh`: Creates the `BQMS_PROJECT`, creates the `BQMS_GCS_BUCKET`,
    enables the necessary services and grants the necessary permissions to
    the `BQMS_DEVELOPER_EMAIL`. Example execution:
    ```shell
    export BQMS_PROJECT="<YOUR_DESIRED_GCP_PROJECT>"
    export BQMS_DEVELOPER_EMAIL="<YOUR_EMAIL>"
    export BQMS_GCS_BUCKET="<YOUR_DESIRED_GCS_BUCKET>"
    export BQMS_GCS_BUCKET_LOCATION="<YOUR_DESIRED_GCS_BUCKET_LOCATION>"
    ./provision.sh
    ```
  - `deprovision.sh`: Deletes the `BQMS_PROJECT`.
  - `clean.sh`: Removes local `preprocessed`, `translated` and `postprocessed` 
    directories.

## Metadata Lookup

In order to properly translate DML and queries, the batch SQL translator needs
to know the data types of all columns that are referenced. This metadata
information can be provided either via DDL or a metadata dump.

### DDL and Metadata Dump

If DDL scripts are available, please include them in the `BQMS_INPUT_PATH` 
directory along with the DML and queries to be translated. If DDL scripts are
not available, please 
[generate a metadata dump as described in the docs][dumper] and include it in
the `BQMS_INPUT_PATH` directory along with the DML and queries to be translated.

### Unqualified References

Often, DML and queries reference table names without specifying which database
or schema they are in. These are known as unqualified references. When the
batch SQL translator encounters unqualified references it will use the
`default_database` and `schema_search_path` values located in
[config.yaml](#configyaml) in order to look up the unqualified references in the
metadata and determine their definition (e.g. column names and data types). For
more details on these configuration values, please see their docs:

- [default_database][default_database]
- [schema_search_path][schema_search_path]

## Pre and Postprocessing

### Handling Macro/Templating Languages

Currently, the batch SQL translator only accepts syntactically valid SQL and
Teradata BTEQ. Often, SQL and BTEQ files contain syntactically invalid macors
or template variables such as [Jinja variables][jinja] or
[environment variables][env_vars]. These macros/template variables must be
replaced with syntactically valid values. This is accomplished using a YAML
file that is passed as the `BQMS_MACRO_MAPPING_PATH` environment variable. The
YAML file maps glob patterns to macro name/value pairs. If a script path matches
a glob pattern, then each macro name associated with the glob pattern will be
replaced with (i.e. expanded to) its respective macro value upon preprocessig of
the input script. Upon postprocessing of the translated output script, the
reverse operation is performed and each macro value is replaced with (i.e.
unexpanded to) its respective macro name. For example, given the following YAML
mapping:

```yaml
macros:
  '*.sql':
    '${MACRO_1}': 'my_col'
  'date-truncation.sql':
    '${demo_date_truncation_table}': 'demo_date_table'
    '${demo_date}': '2022-09-01'
```

and the following input script named `test.sql`:

```sql
select ${MACRO_1}
from foo
```

the result of preprocessing will be:

```sql
select my_col
from foo
```

Once translated, `my_col` will be unexpanded back to `${MACRO_1}` during 
postprocessing.

More advanced macro replacement schemes are possible. Please see the 
`custom_pattern_macros_example` function in the `bqms_run/hooks.py` module for
an example.

### Custom Pre and Postprocessing Logic

Beyond handling macro/templating languages, it is common to require custom pre
and postprocessing logic that is specific to a particular organization or a
particular set of scripts. For example, Netezza [nzsql][nzsql] scripts usually
contain [slash commands][nzsql_slash_cmds] that do not need to be translated to
BigQuery and can be commented out. The `bqms_run/hooks.py` module supports 
custom user-defined per-script processing logic via a `preprocess` function that
is called once for each input script and a `postprocess` function that is called
once for each translated output script. For example, to comment out nzsql slash
commands in the input, the `preprocess` function can be modified as follows:

```python
def preprocess(path: Path, text: str) -> str:
    """Preprocesses input via user-defined code before submitting it to BQMS.

    Args:
        path: A bqms_run.paths.Path representing the relative path of the input to be
            preprocessed.
        text: A string representing the contents of the input to be
            preprocessed.

    Returns:
        A string representing the preprocesssed input.
    """
    return re.sub(r'^\\', r'--\\', text)
```

## Renaming SQL Identifiers

The batch SQL translator supports renaming SQL identifiers via a feature called
object name mapping. This feature can be configured by setting the 
`BQMS_OBJECT_NAME_MAPPING_PATH` environment variable to the path of a JSON file
that contains name mapping rules. Please see the
[official object name mapping docs][ONM] for details on how to specify name
mapping rules via JSON.

## Deploying to Cloud Run

The `examples` directory discussed in [Recommended Usage](#recommended-usage)
provides `provision.sh` and `run.sh` scripts to simplify the process of
executing the CLI on Cloud Run.

A prerequisite to executing the `provision.sh` and `run.sh` scripts is building
and publishing a container image to an [artifact registry][artifact_registry].
This can be accomplished by running the following command from the same
directory as this `README`:

```shell
gcloud --project="${BQMS_ARTIFACT_PROJECT}" builds submit \
  -t "${BQMS_ARTIFACT_TAG}"
```

For more information on building and publishing Cloud Run container images,
please see the [docs][cloud_run_build].

Once a container image has been published to an artifact registry, the 
`provision.sh` script can be executed to create the `BQMS_PROJECT`, create the 
`BQMS_GCS_BUCKET`, enable the necessary services, create the Cloud Run service
account and grant the necessary permissions.

Example execution:

```shell
export BQMS_PROJECT="<YOUR_DESIRED_GCP_PROJECT>"
export BQMS_DEVELOPER_EMAIL="<YOUR_EMAIL>"
export BQMS_GCS_BUCKET="<YOUR_DESIRED_GCS_BUCKET>"
export BQMS_GCS_BUCKET_LOCATION="<YOUR_DESIRED_GCS_BUCKET_LOCATION>"
export BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME="<YOUR_DESIRED_CLOUD_RUN_SA_NAME>"
export BQMS_ARTIFACT_PROJECT="<YOUR_ARTIFACT_PROJECT>"
./provision.sh
```

Finally, the `run.sh` script can be executed to sync the local input and config
files to GCS, create the Cloud Run job with the necessary environment variables,
execute the Cloud Run job, gather and print the logs from the Cloud Run job and
sync the preprocessed, translated and postprocessed files from GCS to the local
filesystem.

Example execution:

```shell
export BQMS_VERBOSE="False"
export BQMS_MULTITHREADED="True"
export BQMS_PROJECT="<YOUR_GCP_PROJECT>"
export BQMS_GCS_BUCKET="<YOUR_GCS_BUCKET>"
export BQMS_CLOUD_RUN_REGION="<YOUR_DESIRED_CLOUD_RUN_REGION>"
export BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME="<YOUR_DESIRED_CLOUD_RUN_SA_NAME>"
export BQMS_CLOUD_RUN_JOB_NAME="<YOUR_DESIRED_CLOUD_RUN_JOB_NAME"
export BQMS_CLOUD_RUN_ARTIFACT_TAG="<YOUR_CLOUD_RUN_ARTIFACT_TAG>"
./run.sh
```

<!-- markdownlint-disable line-length -->

[BQMS]: https://cloud.google.com/bigquery/docs/migration-intro
[batch SQL translator]: https://cloud.google.com/bigquery/docs/batch-sql-translator
[ONM]: https://cloud.google.com/bigquery/docs/output-name-mapping
[dumper]: https://cloud.google.com/bigquery/docs/generate-metadata
[default_database]: https://cloud.google.com/bigquery/docs/output-name-mapping#default_database
[schema_search_path]: https://cloud.google.com/bigquery/docs/output-name-mapping#default_schema
[jinja]: https://docs.getdbt.com/docs/build/jinja-macros
[env_vars]: https://stackoverflow.com/a/46535986
[nzsql]: https://www.ibm.com/docs/en/psfa/7.2.1?topic=overview-nzsql-command
[nzsql_slash_cmds]: https://www.ibm.com/docs/en/psfa/7.2.1?topic=information-commonly-used-nzsql-internal-slash-commands
[artifact_registry]: https://cloud.google.com/artifact-registry
[cloud_run_build]: https://cloud.google.com/run/docs/building/containers
