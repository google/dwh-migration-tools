#!/bin/bash -ex
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


SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

gcloud config set project "${BQMS_PROJECT}"

case $(echo "${BQMS_MULTITHREADED}" | tr '[:upper:]' '[:lower:]') in
  "true"|"1"|"t") MULTITHREADED="true"
esac

BQMS_GCS_PREFIX=$(date +%s%N  )-$(tr -dc A-Za-z0-9 </dev/urandom \
    | tr '[:upper:]' '[:lower:]' \
    | head -c 13 ; echo '' )

if [[ -n "${BQMS_CLOUD_RUN_JOB_NAME}" ]]
then
    gsutil ${MULTITHREADED:+ -m} \
        rsync -r -d "${SCRIPT_DIR}" "gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}"

    export BQMS_INPUT_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/input"
    export BQMS_PREPROCESSED_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/preprocessed"
    export BQMS_TRANSLATED_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/translated"
    export BQMS_POSTPROCESSED_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/postprocessed"

    BQMS_RELATIVE_MACRO_MAPPING_PATH="config/macros_mapping.yaml"
    if [[ -f "${SCRIPT_DIR}/${BQMS_RELATIVE_MACRO_MAPPING_PATH}" ]]; then
        export BQMS_MACRO_MAPPING_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/${BQMS_RELATIVE_MACRO_MAPPING_PATH}"
    fi

    BQMS_RELATIVE_OBJECT_NAME_MAPPING_PATH="config/object_name_mapping.json"
    if [[ -f "${SCRIPT_DIR}/${BQMS_RELATIVE_OBJECT_NAME_MAPPING_PATH}" ]]; then
        export BQMS_OBJECT_NAME_MAPPING_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/${BQMS_RELATIVE_OBJECT_NAME_MAPPING_PATH}"
    fi

    BQMS_CLOUD_RUN_SERVICE_ACCOUNT="${BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME}@${BQMS_PROJECT}.iam.gserviceaccount.com"

    gcloud beta run jobs describe "${BQMS_CLOUD_RUN_JOB_NAME}" \
        --region="${BQMS_CLOUD_RUN_REGION}" \
        || gcloud beta run jobs create "${BQMS_CLOUD_RUN_JOB_NAME}" \
            --image="${BQMS_CLOUD_RUN_ARTIFACT_TAG}" \
            --region="${BQMS_CLOUD_RUN_REGION}" \
            --service-account="${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}"

    gcloud beta run jobs update "${BQMS_CLOUD_RUN_JOB_NAME}" \
        --image="${BQMS_CLOUD_RUN_ARTIFACT_TAG}" \
        --region="${BQMS_CLOUD_RUN_REGION}" \
        --service-account="${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}" \
        ${BQMS_VERBOSE:+ --set-env-vars="BQMS_VERBOSE=${BQMS_VERBOSE}"} \
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
        ${BQMS_MACRO_MAPPING_PATH:+ --set-env-vars="BQMS_MACRO_MAPPING_PATH=${BQMS_MACRO_MAPPING_PATH}"} \
        ${BQMS_OBJECT_NAME_MAPPING_PATH:+ --set-env-vars="BQMS_OBJECT_NAME_MAPPING_PATH=${BQMS_OBJECT_NAME_MAPPING_PATH}"}

    gcloud beta run jobs execute "${BQMS_CLOUD_RUN_JOB_NAME}" \
        --region="${BQMS_CLOUD_RUN_REGION}" \
        --wait

    gsutil ${MULTITHREADED:+ -m} \
        rsync -r -d "gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}" "${SCRIPT_DIR}"
else
    export BQMS_INPUT_PATH="${SCRIPT_DIR}/input"
    export BQMS_PREPROCESSED_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/preprocessed"
    export BQMS_TRANSLATED_PATH="gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/translated"
    export BQMS_POSTPROCESSED_PATH="${SCRIPT_DIR}/postprocessed"

    BQMS_RELATIVE_MACRO_MAPPING_PATH="config/macros_mapping.yaml"
    if [[ -f "${SCRIPT_DIR}/${BQMS_RELATIVE_MACRO_MAPPING_PATH}" ]]; then
        export BQMS_MACRO_MAPPING_PATH="${SCRIPT_DIR}/${BQMS_RELATIVE_MACRO_MAPPING_PATH}"
    fi

    BQMS_RELATIVE_OBJECT_NAME_MAPPING_PATH="config/object_name_mapping.json"
    if [[ -f "${SCRIPT_DIR}/${BQMS_BQMS_RELATIVE_OBJECT_NAME_MAPPING_PATH}" ]]; then
        export BQMS_OBJECT_NAME_MAPPING_PATH="${SCRIPT_DIR}/${BQMS_RELATIVE_OBJECT_NAME_MAPPING_PATH}"
    fi

    bqms-cloud-run

    mkdir -p "${SCRIPT_DIR}/preprocessed"
    gsutil ${MULTITHREADED:+ -m} \
        rsync -r -d "gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/preprocessed" "${SCRIPT_DIR}/preprocessed"
    mkdir -p "${SCRIPT_DIR}/translated"
    gsutil ${MULTITHREADED:+ -m} \
        rsync -r -d "gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREFIX}/translated" "${SCRIPT_DIR}/translated"
fi
