# BQMS Cloud Run Container

All that is required is a project. The `run.sh` script will enable the required
services and then create the required resources.

```shell
cd examples/teradata/sql

export BQMS_VERBOSE="True"
export BQMS_PROJECT="ajwelch-bqms-test-004"
export BQMS_TRANSLATION_REGION="us"
export BQMS_TRANSLATION_TYPE="Translation_Teradata2BQ"
export BQMS_SOURCE_ENV_DEFAULT_DATABASE="default_db"
export BQMS_SOURCE_ENV_SCHEMA_SEARCH_PATH="library,foo"
export BQMS_GCS_BUCKET=$BQMS_PROJECT
export BQMS_CLOUD_RUN_REGION="us-east4"
export BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME="bqms-sa"
export BQMS_CLOUD_RUN_JOB_NAME="bqms"
export BQMS_ARTIFACT_TAG="us-east4-docker.pkg.dev/ajwelch-bqms-test-004/bqms/bqms:latest"

./run.sh
```
