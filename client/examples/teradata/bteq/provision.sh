#!/bin/bash
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Log message colors.
COLRED='\033[0;31m' # Red
COLLRD='\033[0;91m' # Light Red
COLYLW='\033[0;33m' # Yellow
COLGRN='\033[0;32m' # Green
COLWHT='\033[0;97m' # White
COLRST='\033[0m'    # Text Reset

# Log levels.
CRT_LVL=1
ERR_LVL=2
WRN_LVL=3
INF_LVL=4
DBG_LVL=5

# Set log level.
case $(echo "${BQMS_VERBOSE}" | tr '[:upper:]' '[:lower:]') in # smash to lowercase
    "true"|"1"|"t") VERBOSE=5;;
    *) VERBOSE=4;;
esac

# Logging helper funcs.
function log() {
    if [ $VERBOSE -ge $VERB_LVL ]; then
        DATESTRING=`date +"%Y-%m-%d %H:%M:%S,%3N"`
        echo -e "$DATESTRING: $@"
    fi
}
function log_fatal () { VERB_LVL=$CRT_LVL log "${COLRED}FATAL:${COLRST} $@" ;}
function log_error () { VERB_LVL=$ERR_LVL log "${COLLRD}ERROR:${COLRST} $@" ;}
function log_warn ()  { VERB_LVL=$WRN_LVL log "${COLYLW}WARNING:${COLRST} $@" ;}
function log_info ()  { VERB_LVL=$INF_LVL log "${COLGRN}INFO:${COLRST} $@" ;}
function log_debug () { VERB_LVL=$DBG_LVL log "${COLWHT}DEBUG:${COLRST} $@" ;}
function log_dump_var () { for var in $@ ; do log_debug "$var=${!var}" ; done }
function log_exec() {
    # This function takes 3 arguments in order:
    # 1. A pre-execution message.
    # 2. An error message.
    # 3. A command to execute.
    # First, the pre-execution message is logged at the INFO level. Then, the
    # command is executed and the output and return code are captured. If the
    # return code is 0, the output is logged at DEBUG level. If the return code
    # is anything other than 0, the error message is logged at FATAL level, the
    # output is logged at FATAL level and exit is called with the return code.
    ARGS=("$@")
    log_info "${ARGS[0]}"
    OUTPUT=$("${ARGS[@]:2}" 2>&1)
    RETURN_CODE=$?
    case $RETURN_CODE in
        0) log_debug "${OUTPUT}";;
        *) log_fatal "${ARGS[1]}"; log_fatal "${OUTPUT}"; exit $RETURN_CODE;;
    esac
}

# Create BQMS project.
log_exec "Creating project: ${BQMS_PROJECT}." \
    "Could not create project: ${BQMS_PROJECT}." \
    gcloud projects create "${BQMS_PROJECT}"

# Optionally link project with a billing account.
if [[ -n "${BQMS_PROJECT_BILLING_ACCOUNT_ID}" ]]
then
    log_exec "Linking billing account ID: ${BQMS_PROJECT_BILLING_ACCOUNT_ID}." \
        "Could not link billing account ID: ${BQMS_PROJECT_BILLING_ACCOUNT_ID}." \
        gcloud beta billing projects link "${BQMS_PROJECT}" \
            --billing-account="${BQMS_PROJECT_BILLING_ACCOUNT_ID}"
fi

# Ensure gcloud is using BQMS_PROJECT as the active project.
log_exec "Setting project to: ${BQMS_PROJECT}." \
    "Could not set project to: ${BQMS_PROJECT}." \
    gcloud config set project "${BQMS_PROJECT}"

# Enable BQMS in the project.
log_exec "Enabling service: bigquerymigration.googleapis.com." \
    "Could not enable service: bigquerymigration.googleapis.com." \
        gcloud services enable bigquerymigration.googleapis.com

# Create the GCS bucket for BQMS input/output.
log_exec "Creating bucket: ${BQMS_GCS_BUCKET}." \
    "Could not create bucket: ${BQMS_GCS_BUCKET}." \
    gsutil mb -l "${BQMS_GCS_BUCKET_LOCATION}" "gs://${BQMS_GCS_BUCKET}"

# Ensure the BQMS developer has admin rights to objects in GCS bucket.
log_exec "Granting storage.objectAdmin role to ${BQMS_DEVELOPER_EMAIL} on ${BQMS_GCS_BUCKET} bucket." \
    "Could not grant storage.objectAdmin role to ${BQMS_DEVELOPER_EMAIL} on ${BQMS_GCS_BUCKET} bucket." \
    gsutil iam ch "user:${BQMS_DEVELOPER_EMAIL}:objectAdmin" "gs://${BQMS_GCS_BUCKET}"

# Ensure the BQMS developer can view logs in the BQMS project.
log_exec "Granting logging.viewer role to ${BQMS_DEVELOPER_EMAIL}." \
    "Could not grant logging.viewer role to ${BQMS_DEVELOPER_EMAIL}." \
    gcloud projects add-iam-policy-binding "${BQMS_PROJECT}" \
        --member="user:${BQMS_DEVELOPER_EMAIL}" \
        --role=roles/logging.viewer

# If BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME env var is set, then assume the Python
# tool will be executed via a Cloud Run job. Otherwise, assume local execution.
if [[ -n "${BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME}" ]]
then
    # Enable Cloud Run in the project.
    log_exec "Enabling service: run.googleapis.com." \
        "Could not enable service: run.googleapis.com." \
            gcloud services enable run.googleapis.com

    BQMS_CLOUD_RUN_SERVICE_ACCOUNT="${BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME}@${BQMS_PROJECT}.iam.gserviceaccount.com"

    # Create the service account the Cloud Run container will run as.
    log_exec "Creating service account: ${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}." \
        "Could not create service account: ${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}." \
        gcloud iam service-accounts create "${BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME}" \
            --display-name="BQMS Cloud Run Service Account"

    # TODO: The following goes away when we have a public image.
    BQMS_PROJECT_NUMBER=$(gcloud projects list \
        --filter="${BQMS_PROJECT}" \
        --format="value(PROJECT_NUMBER)")
    BQMS_CLOUD_RUN_SERVICE_AGENT="service-${BQMS_PROJECT_NUMBER}@serverless-robot-prod.iam.gserviceaccount.com"
    log_exec "Granting artifactregistry.reader role to ${BQMS_CLOUD_RUN_SERVICE_AGENT}." \
        "Could not grant artifactregistry.reader role to ${BQMS_CLOUD_RUN_SERVICE_AGENT}." \
        gcloud projects add-iam-policy-binding "${BQMS_ARTIFACT_PROJECT}" \
            --member="serviceAccount:${BQMS_CLOUD_RUN_SERVICE_AGENT}" \
            --role=roles/artifactregistry.reader

    # Ensure the Cloud Run service account can write to Cloud Logging.
    log_exec "Granting logging.logWriter role to ${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}." \
        "Could not grant logging.logWriter role to ${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}." \
        gcloud projects add-iam-policy-binding "${BQMS_PROJECT}" \
            --member="serviceAccount:${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}" \
            --role=roles/logging.logWriter

    # Ensure the Cloud Run service account has admin rights to objects in GCS bucket.
    log_exec "Granting storage.objectAdmin role to ${BQMS_CLOUD_RUN_SERVICE_ACCOUNT} on ${BQMS_GCS_BUCKET}." \
        "Could not grant storage.objectAdmin role to ${BQMS_CLOUD_RUN_SERVICE_ACCOUNT} on ${BQMS_GCS_BUCKET}." \
        gsutil iam ch "serviceAccount:${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}:objectAdmin" "gs://${BQMS_GCS_BUCKET}"

    # Ensure the Cloud Run service account can create BQMS jobs.
    log_exec "Granting bigquerymigration.editor role to ${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}." \
        "Could not grant bigquerymigration.editor role to ${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}." \
        gcloud projects add-iam-policy-binding "${BQMS_PROJECT}" \
            --member="serviceAccount:${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}" \
            --role=roles/bigquerymigration.editor

    # Ensure the BQMS developer can assign the service account to the Cloud Run container.
    log_exec "Granting iam.serviceAccountUser role to ${BQMS_DEVELOPER_EMAIL}." \
        "Could not grant iam.serviceAccountUser role to ${BQMS_DEVELOPER_EMAIL}." \
        gcloud iam service-accounts add-iam-policy-binding "${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}" \
            --member="user:${BQMS_DEVELOPER_EMAIL}" \
            --role=roles/iam.serviceAccountUser

    # Ensure the BQMS developer can run the Cloud Run container.
    log_exec "Granting run.developer role to ${BQMS_DEVELOPER_EMAIL}." \
        "Could not grant run.developer role to ${BQMS_DEVELOPER_EMAIL}." \
        gcloud projects add-iam-policy-binding "${BQMS_PROJECT}" \
            --member="user:${BQMS_DEVELOPER_EMAIL}" \
            --role=roles/run.developer
else
    # Ensure the BQMS developer can create BQMS jobs.
    log_exec "Granting bigquerymigration.editor role to ${BQMS_DEVELOPER_EMAIL}." \
        "Could not grant bigquerymigration.editor role to ${BQMS_DEVELOPER_EMAIL}." \
        gcloud projects add-iam-policy-binding "${BQMS_PROJECT}" \
            --member="user:${BQMS_DEVELOPER_EMAIL}" \
            --role=roles/bigquerymigration.editor

    # NOTE: Running the Python tool as a Google Cloud User Account (not Service
    # Account) may yield the following warning:
    #
    # UserWarning: Your application has authenticated using end user credentials
    # from Google Cloud SDK without a quota project. You might receive a
    # "quota exceeded" or "API not enabled" error. We recommend you rerun
    # `gcloud auth application-default login` and make sure a quota project is
    # added. Or you can use service accounts instead. For more information about
    # service accounts, see https://cloud.google.com/docs/authentication/
    #
    # Setting a quota project will resolve this warning and can be done with the
    # following command:
    #
    # gcloud auth application-default set-quota-project "$BQMS_PROJECT"
    #
    # This requires the following service and IAM policy binding.
    log_exec "Enabling service: cloudresourcemanager.googleapis.com." \
        "Could not enable service: cloudresourcemanager.googleapis.com." \
        gcloud services enable cloudresourcemanager.googleapis.com
    log_exec "Granting serviceusage.serviceUsageConsumer role to ${BQMS_DEVELOPER_EMAIL}." \
        "Could not grant serviceusage.serviceUsageConsumer role to ${BQMS_DEVELOPER_EMAIL}." \
        gcloud projects add-iam-policy-binding "${BQMS_PROJECT}" \
            --member="user:${BQMS_DEVELOPER_EMAIL}" \
            --role=roles/serviceusage.serviceUsageConsumer
fi
