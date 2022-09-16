# BQMS Cloud Run Container

```shell
cd examples/teradata/sql
```

## Local

Provision:

```shell
export BQMS_PROJECT="bqms"
export BQMS_PROJECT_BILLING_ACCOUNT_ID="######-######-######"
export BQMS_DEVELOPER_EMAIL="dev@google.com"
export BQMS_GCS_BUCKET=$BQMS_PROJECT

./provision.sh
```

Run:

```shell
export BQMS_VERBOSE="True"
export BQMS_MULTITHREADED="True"
export BQMS_PROJECT="bqms"
export BQMS_GCS_BUCKET=$BQMS_PROJECT
export BQMS_TRANSLATION_REGION="us"
export BQMS_TRANSLATION_TYPE="Translation_Teradata2BQ"
export BQMS_SOURCE_ENV_DEFAULT_DATABASE="default_db"
export BQMS_SOURCE_ENV_SCHEMA_SEARCH_PATH="library,foo"

./run.sh
````

Clean/deprovision:

```shell
./clean.sh
./deprovision.sh
```

## Cloud Run

Provision:

```shell
export BQMS_PROJECT="bqms"
export BQMS_PROJECT_BILLING_ACCOUNT_ID="######-######-######"
export BQMS_DEVELOPER_EMAIL="dev@google.com"
export BQMS_GCS_BUCKET=$BQMS_PROJECT
export BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME="bqms-sa"
# TODO: The following goes away when we have a public image.
export BQMS_ARTIFACT_PROJECT="bqms-artifact"

./provision.sh
```

Run:

```shell
export BQMS_VERBOSE="True"
export BQMS_MULTITHREADED="True"
export BQMS_PROJECT="bqms"
export BQMS_GCS_BUCKET=$BQMS_PROJECT
export BQMS_CLOUD_RUN_REGION="us-east4"
export BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME="bqms-sa"
export BQMS_CLOUD_RUN_JOB_NAME="bqms"
export BQMS_CLOUD_RUN_ARTIFACT_TAG="us-east4-docker.pkg.dev/bqms-artifact/bqms/bqms:latest"
export BQMS_TRANSLATION_REGION="us"
export BQMS_TRANSLATION_TYPE="Translation_Teradata2BQ"
export BQMS_SOURCE_ENV_DEFAULT_DATABASE="default_db"
export BQMS_SOURCE_ENV_SCHEMA_SEARCH_PATH="library,foo"

./run.sh
```

Clean/deprovision:

```shell
./clean.sh
./deprovision.sh
```
