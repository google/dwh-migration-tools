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


gcloud projects create "${BQMS_PROJECT}"

gcloud beta billing projects link "${BQMS_PROJECT}" \
    --billing-account="${BQMS_PROJECT_BILLING_ACCOUNT_ID}"

gcloud config set project "${BQMS_PROJECT}"

gcloud services enable bigquerymigration.googleapis.com

gsutil mb -l "${BQMS_GCS_BUCKET_LOCATION}" "gs://${BQMS_GCS_BUCKET}"

gsutil iam ch "user:${BQMS_DEVELOPER_EMAIL}:objectAdmin" "gs://${BQMS_GCS_BUCKET}"

gcloud projects add-iam-policy-binding "${BQMS_PROJECT}" \
    --member="user:${BQMS_DEVELOPER_EMAIL}" \
    --role=roles/logging.viewer

if [[ -n "${BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME}" ]]
then
    gcloud services enable run.googleapis.com

    BQMS_CLOUD_RUN_SERVICE_ACCOUNT="${BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME}@${BQMS_PROJECT}.iam.gserviceaccount.com"

    gcloud iam service-accounts create "${BQMS_CLOUD_RUN_SERVICE_ACCOUNT_NAME}" \
        --display-name="BQMS Cloud Run Service Account"

    # TODO: The following goes away when we have a public image.
    BQMS_PROJECT_NUMBER=$(gcloud projects list \
        --filter="${BQMS_PROJECT}" \
        --format="value(PROJECT_NUMBER)")
    BQMS_CLOUD_RUN_SERVICE_AGENT="service-${BQMS_PROJECT_NUMBER}@serverless-robot-prod.iam.gserviceaccount.com"
    gcloud projects add-iam-policy-binding "${BQMS_ARTIFACT_PROJECT}" \
        --member="serviceAccount:${BQMS_CLOUD_RUN_SERVICE_AGENT}" \
        --role=roles/artifactregistry.reader

    gsutil iam ch "serviceAccount:${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}:objectAdmin" "gs://${BQMS_GCS_BUCKET}"

    gcloud projects add-iam-policy-binding "${BQMS_PROJECT}" \
        --member="serviceAccount:${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}" \
        --role=roles/bigquerymigration.editor

    gcloud iam service-accounts add-iam-policy-binding "${BQMS_CLOUD_RUN_SERVICE_ACCOUNT}" \
        --member="user:${BQMS_DEVELOPER_EMAIL}" \
        --role=roles/iam.serviceAccountUser

    gcloud projects add-iam-policy-binding "${BQMS_PROJECT}" \
        --member="user:${BQMS_DEVELOPER_EMAIL}" \
        --role=roles/run.developer
else
    gcloud projects add-iam-policy-binding "${BQMS_PROJECT}" \
        --member="user:${BQMS_DEVELOPER_EMAIL}" \
        --role=roles/bigquerymigration.editor
fi
