# BigQuery Migration Service Batch SQL Translator CLI

This directory contains the Batch SQL Translator CLI, a command line client
that simplifies the process of using the [batch SQL translator][batch SQL translator]
of the [BigQuery Migration Service][BQMS] to translate from a wide variety of
SQL dialects to Google Standard SQL. This client provides a framework that
handles pre and postprocessing of SQL files, upload/download of SQL files
to/from GCS, and invoking the GCP SQL translation API.

Read in the Google Cloud documentation [how to submit a translation job using
this client][how to submit].

- [Installation](#installation)
- [Quickstart](#quickstart)
- [config.yaml](#configyaml)
- [Running Using `run.sh`](#running-using-runsh)
- [Running Using `bqms-run` Directly](#running-using-bqms-run-directly)
  - [Environment Variables](#environment-variables)
- [Metadata Lookup](#metadata-lookup)
  - [DDL and Metadata Dump](#ddl-and-metadata-dump)
  - [Unqualified References](#unqualified-references)
- [Pre and Postprocessing](#pre-and-postprocessing)
  - [Handling Macro/Templating Languages](#handling-macrotemplating-languages)
  - [Extraction of Heredoc SQL Statements from KSH Inputs](#extraction-of-heredoc-sql-statements-from-ksh-inputs)
  - [Custom Pre and Postprocessing Logic](#custom-pre-and-postprocessing-logic)
- [Renaming SQL Identifiers](#renaming-sql-identifiers)
- [Deploying to Cloud Run](#deploying-to-cloud-run)
- [Performance](#performance)

## Installation

Preferred OS: Linux or macOS. Windows usage may be possible through PowerShell,
but it is not officially supported.

**Download and extract [the latest release zip `dwh-migration-tools-vX.X.X.zip`](https://github.com/google/dwh-migration-tools/releases/latest).**

### Python

Python &ge; 3.7.2 is required.
You can check whether you have a recent enough version of Python installed by
running the command `python3 --version`.

You must also have the Python `pip` and `venv` modules installed. Altogether,
the distribution-specific commands to install these are:

* Debian-based distros: `sudo apt install python3-pip python3-venv`
* Red Hat-based distros: `sudo yum install python38 python38-pip` (for e.g. Python
  3.8)

For MacOS, install python from https://www.python.org/downloads/macos/. For Windows,
install python from https://www.python.org/downloads/windows/. Run
`python3 -m pip install --upgrade pip` to install pip.

### Gcloud CLI

Gcloud is required.
You can install from https://cloud.google.com/sdk/docs/install-sdk.
Run `gcloud init` and `gcloud auth application-default login` to initialize and authorize.

### Support for Encodings other than UTF-8

If all of the files you wish to translate are UTF-8 encoded
(this is commonly the case), you can skip this section.
Otherwise, you will need to install additional system dependencies:

* Debian-based distros: `sudo apt install pkg-config libicu-dev`
* RedHat-based distros: `sudo yum install gcc gcc-c++ libicu-devel
  python38-devel`

**You must also remember**, upon reaching the step to `pip install` further down
in the Quickstart section below, to use this command instead:

```shell
pip install ../dwh-migration-tools/client[icu]
```

### GCP

You need a GCP project and a Google Cloud Storage bucket to use for uploading
your input SQL files and downloading the translated output. [Learn how to
create a GCS bucket manually][creating buckets], or see the [instructions for
using `provision.sh`](#running-using-runsh) to automatically provision a
bucket for translation.

## Quickstart

1. Download the repo from [google/dwh-migration-tools] in your choice of 
preference. If you download a zip, make sure to change the name of the 
folder to "dwh-migration-tools".
2. Run the following commands, making the appropriate substitutions.

```shell
# Copy the example project directory to a project directory of your own 
# (preferably outside of the source tree to make pulling source updates easier).
cp -R dwh-migration-tools/client/examples/teradata/sql <YOUR_PROJECT_DIRECTORY>

# Change directory to your project directory.
cd <YOUR_PROJECT_DIRECTORY>

# Create a virtualenv and install the Python CLI.
python3 -m venv venv
source venv/bin/activate
pip install ../dwh-migration-tools/client

# Remove the example input files from the input directory.
rm -rf input/*
# Copy the files you would like to translate into the input directory.
cp <YOUR_INPUT_FILES> input/

# Edit the config/config.yaml file appropriately as described in the
# "config.yaml" section below.

# Set the appropriate environment variables as described in the
# "Running Using `run.sh`" section below.
export BQMS_VERBOSE="False"
export BQMS_MULTITHREADED="True"
export BQMS_PROJECT="<YOUR_GCP_PROJECT>"
export BQMS_GCS_BUCKET="<YOUR_GCS_BUCKET>"

# Run the translation.
./run.sh
```

## config.yaml

The `config.yaml` file specifies the translation type (i.e. source and target
dialects), translation location and default values for
[unqualified references](#unqualified-references).

The `run.sh` wrapper script looks for `config.yaml` in the `config` directory
in which it is running. If you are using `bqms-run` directly instead, it
requires a file path be passed as the `BQMS_CONFIG_PATH` environment variable.

Example `config.yaml` file:

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

## Running Using `run.sh`

There are two ways to run the CLI: via the wrapping `run.sh` script or
directly. Using the `run.sh` wrapper is recommended unless you have advanced
usage requirements. Basic usage for running via `run.sh` is shown in the
[quickstart](#quickstart) above.

The `examples` directory includes a recommended directory structure for using
the CLI:

```
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
script within the directory. Doing so provides the following benefits over
[running using `bqms-run` directly](#running-using-bqms-run-directly):

- Automatic setting of most [environment variables](#environment-variables)
  based on the recommended directory structure. Modifying the
  [config.yaml](#configyaml) file is still required.
- Downloading of intermediate pre and postprocessed files from GCS for
  debugging as described in [Pre and Postprocessing](#pre-and-postprocessing).
- Execution via Cloud Run as described in
  [Deploying to Cloud Run](#deploying-to-cloud-run).

The `run.sh` script accepts the following environment variables when
run locally:

- `BQMS_PROJECT` **(required)**: The project that the translation job will be
  run in.
- `BQMS_GCS_BUCKET` **(required)**: The GCS bucket where preprocessed and
  translated code will be written to.
- `BQMS_SYNC_INTERMEDIATE_FILES`: Set to `True` to enable syncing of
  intermediate files from GCS e.g. preprocessed files. This is useful for
  debugging pre and postprocessing logic but also requires additional GCS
  downloads.
- `BQMS_MULTITHREADED`: Set to `True` to enable multithreaded
  pre/postprocessing and uploads/downloads.
- `BQMS_VERBOSE`: Set to `True` to enable debug logging.

For a list of environment variables accepted by the `run.sh` script when using
Cloud Run, see [Deploying to Cloud Run](#deploying-to-cloud-run).

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


## Running Using `bqms-run` Directly

Direct execution is suitable for advanced use cases. It requires setting
[environment variables](#environment-variables) and modifying a
[config.yaml](#configyaml) file, then running the `bqms-run` command
installed by this repository's `pip` package.

A Docker container of `bqms-run` with all the necessary dependencies can be
built by running:

```shell
sudo docker build -t bqms-run client
```

See also the instructions for [deploying to Cloud Run](#deploying-to-cloud-run).

### Environment Variables

`bqms-run` accepts the following environment variables:

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
- `BQMS_MULTITHREADED`: Set to `True` to enable multithreaded uploads/downloads.
- `BQMS_GCS_CHECKS`: Set to `True` to enable additional checks that ensure local
   files are in sync with GCS files. **WARNING**: This will generate additional
   network requests that can increase execution time by 2-3x. Most users should
   not set this as it is only required if GCS files are being manipulated by an
   out-of-band process which is highly discouraged.
- `BQMS_VERBOSE`: Set to `True` to enable debug logging.

## Metadata Lookup

In order to properly translate DML and queries, the batch SQL translator needs
to know the data types of all columns that are referenced. This metadata
information can be provided either via DDL or a metadata dump.

### DDL and Metadata Dump

If DDL scripts are available, include them in the `BQMS_INPUT_PATH` directory
along with the DML and queries to be translated. If DDL scripts are
not available, [generate a metadata dump as described in the docs][dumper] and
include it in the `BQMS_INPUT_PATH` directory along with the DML and queries to
be translated.

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
Teradata BTEQ. Often, SQL and BTEQ files contain syntactically invalid macros
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

### Extraction of Heredoc SQL Statements from KSH Inputs

By default, during the preprocessing phase, any input paths ending in
extension `.ksh` will be scanned for heredoc SQL statements. For example,
given the following file named `foo.ksh`:

```shell
#!/bin/ksh
## A Very simple test.
bteq  <<EOF
SELECT 123, 'foo', 456 from bar;
EOF
echo Trying another select.
```

will be preprocessed to:

```sql
SELECT 123, 'foo', 456 from bar;
```

Any KSH files which do not contain heredoc SQL fragments will be preprocessed
to a single SQL comment indicating as much.

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
        path: A bqms_run.paths.Path representing the relative path of the input
            to be preprocessed.
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
[object name mapping docs][ONM] for details on how to specify name mapping
rules via JSON.

## Deploying to Cloud Run

The `examples` directory discussed in [Running Using `run.sh`](#running-using-runsh)
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

## Performance

Performance of this tool has been tested using the `test_teradata_local` test
located at `tests/integration/test_teradata.py`. The test was run on a machine
with the following specs:

<!-- markdownlint-disable line-length -->

```shell
> free -hm

               total        used        free      shared  buff/cache   available
Mem:            94Gi        10Gi        63Gi       146Mi        20Gi        82Gi
Swap:          1.9Gi        32Mi       1.8Gi

> lscpu

Architecture:            x86_64
  CPU op-mode(s):        32-bit, 64-bit
  Address sizes:         46 bits physical, 48 bits virtual
  Byte Order:            Little Endian
CPU(s):                  24
  On-line CPU(s) list:   0-23
Vendor ID:               GenuineIntel
  Model name:            Intel(R) Xeon(R) CPU @ 2.20GHz
    CPU family:          6
    Model:               79
    Thread(s) per core:  2
    Core(s) per socket:  12
    Socket(s):           1
    Stepping:            0
    BogoMIPS:            4400.41
    Flags:               fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ss ht syscall nx pdpe1gb rdtscp lm constant_tsc rep_good nopl xtopology nonstop_tsc
                          cpuid tsc_known_freq pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm abm 3dnowprefetch invpcid_single pti ssbd ibrs
                          ibpb stibp fsgsbase tsc_adjust bmi1 hle avx2 smep bmi2 erms invpcid rtm rdseed adx smap xsaveopt arat md_clear arch_capabilities
Virtualization features: 
  Hypervisor vendor:     KVM
  Virtualization type:   full
Caches (sum of all):     
  L1d:                   384 KiB (12 instances)
  L1i:                   384 KiB (12 instances)
  L2:                    3 MiB (12 instances)
  L3:                    55 MiB (1 instance)
NUMA:                    
  NUMA node(s):          1
  NUMA node0 CPU(s):     0-23
```

<!-- markdownlint-enable line-length -->

### Results

The execution times were as follows:

#### 10k files * 10 KB/file = 100 MB of data

| Threading       | Client     | Service    | Total      |
|-----------------|------------|------------|------------|
| Single-threaded | 0h 17m 36s | 0h 08m 30s | 0h 26m 06s |
| Multi-threaded  | 0h 02m 17s | 0h 11m 56s | 0h 14m 13s |

Single-threaded command:

<!-- markdownlint-disable line-length -->

```shell
BQMS_VERBOSE="True" \
BQMS_MULTITHREADED="False" \
BQMS_PROJECT="<YOUR_PROJECT>" \
BQMS_GCS_BUCKET="<YOUR_GCS_BUCKET>" \
pytest \
  -vv \
  --log-cli-level=DEBUG \
  --log-cli-format="%(asctime)s: %(levelname)s: %(threadName)s: %(filename)s:%(lineno)s: %(message)s" \
  -k "test_teradata_local[10000]" \
  --performance \
  tests/integration
```

<!-- markdownlint-enable line-length -->

Multi-threaded command:

<!-- markdownlint-disable line-length -->

```shell
BQMS_VERBOSE="True" \
BQMS_MULTITHREADED="True" \
BQMS_PROJECT="<YOUR_PROJECT>" \
BQMS_GCS_BUCKET="<YOUR_GCS_BUCKET>" \
pytest \
  -vv \
  --log-cli-level=DEBUG \
  --log-cli-format="%(asctime)s: %(levelname)s: %(threadName)s: %(filename)s:%(lineno)s: %(message)s" \
  -k "test_teradata_local[10000]" \
  --performance \
  tests/integration
```

<!-- markdownlint-enable line-length -->

#### 100k files * 10 KB/file = 1 GB of data (service limit)

| Threading       | Client     | Service    | Total      |
|-----------------|------------|------------|------------|
| Single-threaded | 2h 35m 39s | 1h 40m 00s | 4h 15m 39s |
| Multi-threaded  | 0h 21m 02s | 1h 24m 44s | 1h 45m 46s |

Single-threaded command:

<!-- markdownlint-disable line-length -->

```shell
BQMS_VERBOSE="True" \
BQMS_MULTITHREADED="False" \
BQMS_PROJECT="<YOUR_PROJECT>" \
BQMS_GCS_BUCKET="<YOUR_GCS_BUCKET>" \
pytest \
  -vv \
  --log-cli-level=DEBUG \
  --log-cli-format="%(asctime)s: %(levelname)s: %(threadName)s: %(filename)s:%(lineno)s: %(message)s" \
  -k "test_teradata_local[100000]" \
  --performance \
  tests/integration
```

<!-- markdownlint-enable line-length -->

Multi-threaded command:

<!-- markdownlint-disable line-length -->

```shell
BQMS_VERBOSE="True" \
BQMS_MULTITHREADED="True" \
BQMS_PROJECT="<YOUR_PROJECT>" \
BQMS_GCS_BUCKET="<YOUR_GCS_BUCKET>" \
pytest \
  -vv \
  --log-cli-level=DEBUG \
  --log-cli-format="%(asctime)s: %(levelname)s: %(threadName)s: %(filename)s:%(lineno)s: %(message)s" \
  -k "test_teradata_local[100000]" \
  --performance \
  tests/integration
```

<!-- markdownlint-enable line-length -->

<!-- markdownlint-disable line-length -->

[BQMS]: https://cloud.google.com/bigquery/docs/migration-intro
[batch SQL translator]: https://cloud.google.com/bigquery/docs/batch-sql-translator
[how to submit]: https://cloud.google.com/bigquery/docs/batch-sql-translator#submit_a_translation_job
[creating buckets]: https://cloud.google.com/storage/docs/creating-buckets
[ONM]: https://cloud.google.com/bigquery/docs/output-name-mapping
[dumper]: https://cloud.google.com/bigquery/docs/generate-metadata
[google/dwh-migration-tools]: https://github.com/google/dwh-migration-tools
[default_database]: https://cloud.google.com/bigquery/docs/output-name-mapping#default_database
[schema_search_path]: https://cloud.google.com/bigquery/docs/output-name-mapping#default_schema
[jinja]: https://docs.getdbt.com/docs/build/jinja-macros
[env_vars]: https://stackoverflow.com/a/46535986
[nzsql]: https://www.ibm.com/docs/en/psfa/7.2.1?topic=overview-nzsql-command
[nzsql_slash_cmds]: https://www.ibm.com/docs/en/psfa/7.2.1?topic=information-commonly-used-nzsql-internal-slash-commands
[artifact_registry]: https://cloud.google.com/artifact-registry
[cloud_run_build]: https://cloud.google.com/run/docs/building/containers
