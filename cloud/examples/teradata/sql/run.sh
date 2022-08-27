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

gcloud services enable bigquerymigration.googleapis.com
gcloud services enable run.googleapis.com

gsutil ls -b "gs://${BQMS_GCS_BUCKET}" || gsutil mb "gs://${BQMS_GCS_BUCKET}"

: "${BQMS_GCS_PREFIX=$(date +%s%N  )-$(tr -dc A-Za-z0-9 </dev/urandom \
                  | tr '[:upper:]' '[:lower:]' \
                  | head -c 13 ; echo '' )}"

: "${BQMS_GCS_INPUT_PATH=${BQMS_GCS_PREFIX}/input}"
gsutil -m rsync -r -d "${SCRIPT_DIR}/input" "gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_INPUT_PATH}"

: "${BQMS_GCS_PREPROCESSED_PATH=${BQMS_GCS_PREFIX}/preprocessed}"
: "${BQMS_GCS_TRANSLATED_PATH=${BQMS_GCS_PREFIX}/translated}"
: "${BQMS_GCS_POSTPROCESSED_PATH=${BQMS_GCS_PREFIX}/postprocessed}"

BQMS_RELATIVE_MACRO_MAPPING_PATH="config/macros_mapping.yaml"
BQMS_LOCAL_MACRO_MAPPING_PATH="${SCRIPT_DIR}/${BQMS_RELATIVE_MACRO_MAPPING_PATH}"
if [[ -f "${BQMS_LOCAL_MACRO_MAPPING_PATH}" ]]; then
    : "${BQMS_GCS_MACRO_MAPPING_PATH=${BQMS_GCS_PREFIX}/${BQMS_RELATIVE_MACRO_MAPPING_PATH}}"
    gsutil cp "${SCRIPT_DIR}/config/macros_mapping.yaml" "gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_MACRO_MAPPING_PATH}"
fi

BQMS_RELATIVE_OBJECT_NAME_MAPPING_PATH="config/object_name_mapping.json"
BQMS_LOCAL_OBJECT_NAME_MAPPING_PATH="${SCRIPT_DIR}/${BQMS_RELATIVE_OBJECT_NAME_MAPPING_PATH}"
if [[ -f "${BQMS_LOCAL_OBJECT_NAME_MAPPING_PATH}" ]]; then
    : "${BQMS_GCS_OBJECT_NAME_MAPPING_PATH=${BQMS_GCS_PREFIX}/${BQMS_RELATIVE_OBJECT_NAME_MAPPING_PATH}}"
    gsutil cp "${SCRIPT_DIR}/config/object_name_mapping.json" "gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_OBJECT_NAME_MAPPING_PATH}"
fi

BQMS_SERVICE_ACCOUNT="${BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME}@${BQMS_PROJECT}.iam.gserviceaccount.com"

gcloud iam service-accounts describe "${BQMS_SERVICE_ACCOUNT}" \
  || gcloud iam service-accounts create "${BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME}" \
      --display-name="BQMS cloud run service account"

gsutil iam ch "serviceAccount:${BQMS_SERVICE_ACCOUNT}:objectAdmin" "gs://${BQMS_GCS_BUCKET}"

gcloud projects add-iam-policy-binding ${BQMS_PROJECT} \
    --member=serviceAccount:${BQMS_SERVICE_ACCOUNT} \
    --role=roles/bigquerymigration.editor

gcloud beta run jobs describe "${BQMS_CLOUD_RUN_JOB_NAME}" \
  --region="${BQMS_CLOUD_RUN_REGION}" \
  || gcloud beta run jobs create "${BQMS_CLOUD_RUN_JOB_NAME}" \
      --image="${BQMS_ARTIFACT_TAG}" \
      --region="${BQMS_CLOUD_RUN_REGION}" \
      --service-account="${BQMS_SERVICE_ACCOUNT}"

gcloud beta run jobs update "${BQMS_CLOUD_RUN_JOB_NAME}" \
  --image="${BQMS_ARTIFACT_TAG}" \
  --region="${BQMS_CLOUD_RUN_REGION}" \
  --service-account="${BQMS_SERVICE_ACCOUNT}" \
  ${BQMS_VERBOSE:+ --set-env-vars="BQMS_VERBOSE=${BQMS_VERBOSE}"} \
  --set-env-vars="BQMS_PROJECT=${BQMS_PROJECT}" \
  --set-env-vars="BQMS_TRANSLATION_REGION=${BQMS_TRANSLATION_REGION}" \
  --set-env-vars="BQMS_TRANSLATION_TYPE=${BQMS_TRANSLATION_TYPE}" \
  ${BQMS_SOURCE_ENV_DEFAULT_DATABASE:+ --set-env-vars="BQMS_SOURCE_ENV_DEFAULT_DATABASE=${BQMS_SOURCE_ENV_DEFAULT_DATABASE}"} \
  ${BQMS_SOURCE_ENV_SCHEMA_SEARCH_PATH:+ --set-env-vars="^@^BQMS_SOURCE_ENV_SCHEMA_SEARCH_PATH=${BQMS_SOURCE_ENV_SCHEMA_SEARCH_PATH}"} \
  --set-env-vars="BQMS_GCS_BUCKET=${BQMS_GCS_BUCKET}" \
  --set-env-vars="BQMS_GCS_INPUT_PATH=${BQMS_GCS_INPUT_PATH}" \
  --set-env-vars="BQMS_GCS_PREPROCESSED_PATH=${BQMS_GCS_PREPROCESSED_PATH}" \
  --set-env-vars="BQMS_GCS_TRANSLATED_PATH=${BQMS_GCS_TRANSLATED_PATH}" \
  --set-env-vars="BQMS_GCS_POSTPROCESSED_PATH=${BQMS_GCS_POSTPROCESSED_PATH}" \
  ${BQMS_GCS_MACRO_MAPPING_PATH:+ --set-env-vars="BQMS_GCS_MACRO_MAPPING_PATH=${BQMS_GCS_MACRO_MAPPING_PATH}"} \
  ${BQMS_GCS_OBJECT_NAME_MAPPING_PATH:+ --set-env-vars="BQMS_GCS_OBJECT_NAME_MAPPING_PATH=${BQMS_GCS_OBJECT_NAME_MAPPING_PATH}"}

gcloud beta run jobs execute "${BQMS_CLOUD_RUN_JOB_NAME}" \
  --region="${BQMS_CLOUD_RUN_REGION}" \
  --wait

rm -rf "${SCRIPT_DIR}/preprocessed" && mkdir "${SCRIPT_DIR}/preprocessed"
gsutil -m rsync -r -d "gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_PREPROCESSED_PATH}" "${SCRIPT_DIR}/preprocessed"
rm -rf "${SCRIPT_DIR}/translated" && mkdir "${SCRIPT_DIR}/translated"
gsutil -m rsync -r -d "gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_TRANSLATED_PATH}" "${SCRIPT_DIR}/translated"
rm -rf "${SCRIPT_DIR}/postprocessed" && mkdir "${SCRIPT_DIR}/postprocessed"
gsutil -m rsync -r -d "gs://${BQMS_GCS_BUCKET}/${BQMS_GCS_POSTPROCESSED_PATH}" "${SCRIPT_DIR}/postprocessed"
