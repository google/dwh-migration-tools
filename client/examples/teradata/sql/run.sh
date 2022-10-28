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

# Get the absolute path of this bash script's parent directory.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
log_debug "Script dir: ${SCRIPT_DIR}."

# Ensure gcloud is using BQMS_PROJECT as the active project.
log_exec "Setting project to: ${BQMS_PROJECT}." "Could not set project to: ${BQMS_PROJECT}." \
    gcloud config set project "${BQMS_PROJECT}"

# Determine whether gsutil and the Python tool should use threads.
case $(echo "${BQMS_MULTITHREADED}" | tr '[:upper:]' '[:lower:]') in # smash to lowercase
    "true"|"1"|"t") MULTITHREADED="true"; log_info "Multithreading enabled.";;
    *) log_info "Multithreading disabled.";;
esac

# Determine whether to sync intermediate files (i.e. preprocessed and translated).
case $(echo "${BQMS_SYNC_INTERMEDIATE_FILES}" | tr '[:upper:]' '[:lower:]') in # smash to lowercase
    "true"|"1"|"t") SYNC_INTERMEDIATE_FILES="true"; log_info "Sync intermediate files enabled.";;
    *) log_info "Sync intermediate files disabled.";;
esac

# Generate a prefix of the form 1664253390380560406-uwvu1vpdgsrrl to use as the
# root folder in the GCS bucket to read/write input/output.
BQMS_GCS_PREFIX=$(date +%s%N  )-$(tr -dc A-Za-z0-9 </dev/urandom \
    | tr '[:upper:]' '[:lower:]' \
    | head -c 13 ; echo '' )
log_info "GCS prefix: ${BQMS_GCS_PREFIX}."

# If BQMS_CLOUD_RUN_JOB_NAME env var is set, then execute the Python tool via
# a Cloud Run job. Otherwise, execute the Python tool locally.
if [[ -n "${BQMS_CLOUD_RUN_JOB_NAME}" ]]
then
    # Sync config files and input files to GCS to be processed by Cloud Run job.
    log_exec "Syncing ${SCRIPT_DIR} to gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}." \
        "Could not sync ${SCRIPT_DIR} to gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}." \
            gsutil ${MULTITHREADED:+ -m} \
                rsync -r -d "${SCRIPT_DIR}" "gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}"

    # Build and export path env vars that the Cloud Run Python tool will use.
    export BQMS_INPUT_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/input"
    log_info "Input path: ${BQMS_INPUT_PATH}."
    export BQMS_PREPROCESSED_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/preprocessed"
    log_info "Preprocessed path: ${BQMS_PREPROCESSED_PATH}."
    export BQMS_TRANSLATED_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/translated"
    log_info "Translated path: ${BQMS_TRANSLATED_PATH}."
    export BQMS_POSTPROCESSED_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/postprocessed"
    log_info "Postprocessed path: ${BQMS_POSTPROCESSED_PATH}."

    export BQMS_CONFIG_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/config/config.yaml"
    log_info "Config path: ${BQMS_CONFIG_PATH}."

    # Macro mapping is optional. Check that macro mapping path exists before
    # setting BQMS_MACRO_MAPPING_PATH.
    BQMS_RELATIVE_MACRO_MAPPING_PATH="config/macros_mapping.yaml"
    if [[ -f "${SCRIPT_DIR}/${BQMS_RELATIVE_MACRO_MAPPING_PATH}" ]]; then
        export BQMS_MACRO_MAPPING_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/${BQMS_RELATIVE_MACRO_MAPPING_PATH}"
        log_info "Macro mapping path: ${BQMS_MACRO_MAPPING_PATH}."
    fi

    # Object name mapping is optional. Check that object name mapping path
    # exists before setting BQMS_OBJECT_NAME_MAPPING_PATH.
    BQMS_RELATIVE_OBJECT_NAME_MAPPING_PATH="config/object_name_mapping.json"
    if [[ -f "${SCRIPT_DIR}/${BQMS_RELATIVE_OBJECT_NAME_MAPPING_PATH}" ]]; then
        export BQMS_OBJECT_NAME_MAPPING_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/${BQMS_RELATIVE_OBJECT_NAME_MAPPING_PATH}"
        log_info "Object name mapping path: ${BQMS_OBJECT_NAME_MAPPING_PATH}."
    fi

    # Build Cloud Run service account email.
    BQMS_CLOUD_RUN_SERVICE_ACCOUNT="${BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME}@${BQMS_PROJECT}.iam.gserviceaccount.com"

    # Check if Cloud Run job already exists. If not, create it.
    gcloud beta run jobs describe "${BQMS_CLOUD_RUN_JOB_NAME}" \
        --region="${BQMS_CLOUD_RUN_REGION}" > /dev/null 2>&1
    RETURN_CODE=$?
    if [ $RETURN_CODE -ne 0 ]; then
        log_exec "Creating Cloud Run job: ${BQMS_CLOUD_RUN_JOB_NAME}." \
            "Could not create Cloud Run job: ${BQMS_CLOUD_RUN_JOB_NAME}." \
                gcloud beta run jobs create "${BQMS_CLOUD_RUN_JOB_NAME}" \
                    --image="${BQMS_CLOUD_RUN_ARTIFACT_TAG}" \
                    --region="${BQMS_CLOUD_RUN_REGION}" \
                    --service-account="${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}"
    fi

    # Update Cloud Run job with latest image, region, env vars, etc.
    log_exec "Updating Cloud Run job: ${BQMS_CLOUD_RUN_JOB_NAME}." \
        "Could not update Cloud Run job: ${BQMS_CLOUD_RUN_JOB_NAME}." \
            gcloud beta run jobs update "${BQMS_CLOUD_RUN_JOB_NAME}" \
                --image="${BQMS_CLOUD_RUN_ARTIFACT_TAG}" \
                --region="${BQMS_CLOUD_RUN_REGION}" \
                --service-account="${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}" \
                ${BQMS_VERBOSE:+ --set-env-vars="BQMS_VERBOSE=${BQMS_VERBOSE}"} \
                --set-env-vars="BQMS_CLOUD_LOGGING=True" \
                ${BQMS_MULTITHREADED:+ --set-env-vars="BQMS_MULTITHREADED=${BQMS_MULTITHREADED}"} \
                --set-env-vars="BQMS_PROJECT=${BQMS_PROJECT}" \
                --set-env-vars="BQMS_TRANSLATION_REGION=${BQMS_TRANSLATION_REGION}" \
                --set-env-vars="BQMS_TRANSLATION_TYPE=${BQMS_TRANSLATION_TYPE}" \
                ${BQMS_SOURCE_ENV_DEFAULT_DATABASE:+ --set-env-vars="BQMS_SOURCE_ENV_DEFAULT_DATABASE=${BQMS_SOURCE_ENV_DEFAULT_DATABASE}"} \
                ${BQMS_SOURCE_ENV_SCHEMA_SEARCH_PATH:+ --set-env-vars="^@^BQMS_SOURCE_ENV_SCHEMA_SEARCH_PATH=${BQMS_SOURCE_ENV_SCHEMA_SEARCH_PATH}"} \
                --set-env-vars="BQMS_INPUT_PATH=${BQMS_INPUT_PATH}" \
                --set-env-vars="BQMS_PREPROCESSED_PATH=${BQMS_PREPROCESSED_PATH}" \
                --set-env-vars="BQMS_TRANSLATED_PATH=${BQMS_TRANSLATED_PATH}" \
                --set-env-vars="BQMS_POSTPROCESSED_PATH=${BQMS_POSTPROCESSED_PATH}" \
                --set-env-vars="BQMS_CONFIG_PATH=${BQMS_CONFIG_PATH}" \
                ${BQMS_MACRO_MAPPING_PATH:+ --set-env-vars="BQMS_MACRO_MAPPING_PATH=${BQMS_MACRO_MAPPING_PATH}"} \
                ${BQMS_OBJECT_NAME_MAPPING_PATH:+ --set-env-vars="BQMS_OBJECT_NAME_MAPPING_PATH=${BQMS_OBJECT_NAME_MAPPING_PATH}"}

    # Execute the Cloud Run job.
    BQMS_CLOUD_RUN_JOB_START_TIME=$(date +"%Y-%m-%dT%H:%M:%S%z")
    log_exec "Executing Cloud Run job: ${BQMS_CLOUD_RUN_JOB_NAME}." \
        "Could not execute Cloud Run job: ${BQMS_CLOUD_RUN_JOB_NAME}." \
            gcloud beta run jobs execute "${BQMS_CLOUD_RUN_JOB_NAME}" \
                --region="${BQMS_CLOUD_RUN_REGION}" \
                --wait

    # Print Cloud Run job logs.
    log_info "Reading Cloud Run job logs for: ${BQMS_CLOUD_RUN_JOB_NAME}."
    gcloud logging read \
        "resource.type=cloud_run_job AND resource.labels.job_name=${BQMS_CLOUD_RUN_JOB_NAME} timestamp>=\"${BQMS_CLOUD_RUN_JOB_START_TIME}\"" \
        --order asc --format "value(textPayload)"

    if [[ -n "${SYNC_INTERMEDIATE_FILES}" ]]
    then
        # Sync the translated BQMS output locally so it can be inspected if need be.
        log_exec "Syncing gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX} to ${SCRIPT_DIR}." \
            "Could not sync gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX} to ${SCRIPT_DIR}." \
                gsutil ${MULTITHREADED:+ -m} \
                    rsync -r -d "gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}" "${SCRIPT_DIR}"
    fi
# Execute Python tool locally.
else
    # Build and export path env vars that the Python client will use.
    export BQMS_INPUT_PATH="${SCRIPT_DIR}/input"
    log_info "Input path: ${BQMS_INPUT_PATH}."
    export BQMS_PREPROCESSED_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/preprocessed"
    log_info "Preprocessed path: ${BQMS_PREPROCESSED_PATH}."
    export BQMS_TRANSLATED_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/translated"
    log_info "Translated path: ${BQMS_TRANSLATED_PATH}."
    export BQMS_POSTPROCESSED_PATH="${SCRIPT_DIR}/postprocessed"
    log_info "Postprocessed path: ${BQMS_POSTPROCESSED_PATH}."

    export BQMS_CONFIG_PATH="${SCRIPT_DIR}/config/config.yaml"
    log_info "Config path: ${BQMS_CONFIG_PATH}."

    # Macro mapping is optional. Check that macro mapping path exists before
    # setting BQMS_MACRO_MAPPING_PATH.
    BQMS_LOCAL_MACRO_MAPPING_PATH="${SCRIPT_DIR}/config/macros_mapping.yaml"
    if [[ -f "${BQMS_LOCAL_MACRO_MAPPING_PATH}" ]]; then
        export BQMS_MACRO_MAPPING_PATH="${BQMS_LOCAL_MACRO_MAPPING_PATH}"
        log_info "Macro mapping path: ${BQMS_MACRO_MAPPING_PATH}."
    fi

    # Object name mapping is optional. Check that object name mapping path
    # exists before setting BQMS_OBJECT_NAME_MAPPING_PATH.
    BQMS_LOCAL_OBJECT_NAME_MAPPING_PATH="${SCRIPT_DIR}/config/object_name_mapping.json"
    if [[ -f "${BQMS_LOCAL_OBJECT_NAME_MAPPING_PATH}" ]]; then
        export BQMS_OBJECT_NAME_MAPPING_PATH="${BQMS_LOCAL_OBJECT_NAME_MAPPING_PATH}"
        log_info "Object name mapping path: ${BQMS_OBJECT_NAME_MAPPING_PATH}."
    fi

    # Execute the Python tool.
    log_info "Executing bqms-run command."
    bqms-run || exit $?
    log_info "Completed executing bqms-run command."

    if [[ -n "${SYNC_INTERMEDIATE_FILES}" ]]
    then
        # Sync the preprocessed BQMS input locally so it can be inspected if need
        # be.
        BQMS_LOCAL_PREPROCESSED_PATH="${SCRIPT_DIR}/preprocessed"
        mkdir -p "${BQMS_LOCAL_PREPROCESSED_PATH}"
        log_exec "Syncing ${BQMS_PREPROCESSED_PATH} to ${BQMS_LOCAL_PREPROCESSED_PATH}." \
            "Could not sync ${BQMS_PREPROCESSED_PATH} to ${BQMS_LOCAL_PREPROCESSED_PATH}." \
                gsutil ${MULTITHREADED:+ -m} rsync -r -d "${BQMS_PREPROCESSED_PATH}" \
                    "${BQMS_LOCAL_PREPROCESSED_PATH}"

        # Sync the translated BQMS output locally so it can be inspected if need be.
        BQMS_LOCAL_TRANSLATED_PATH="${SCRIPT_DIR}/translated"
        mkdir -p "${BQMS_LOCAL_TRANSLATED_PATH}"
        log_exec "Syncing ${BQMS_TRANSLATED_PATH} to ${BQMS_LOCAL_TRANSLATED_PATH}." \
            "Could not sync ${BQMS_TRANSLATED_PATH} to ${BQMS_LOCAL_TRANSLATED_PATH}." \
                gsutil ${MULTITHREADED:+ -m} rsync -r -d "${BQMS_TRANSLATED_PATH}" \
                    "${BQMS_LOCAL_TRANSLATED_PATH}"
    fi
fi

log_info "Preprocessing, translation and postprocessing has completed successfully."
