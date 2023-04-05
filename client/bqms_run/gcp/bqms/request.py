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
"""Functions for creating and executing a BQMS migration workflow request."""

import logging
import time
from datetime import datetime
from typing import Optional

from google.cloud.bigquery_migration_v2 import (
    CreateMigrationWorkflowRequest,
    GetMigrationWorkflowRequest,
    MigrationServiceClient,
    MigrationTask,
    MigrationWorkflow,
    ObjectNameMappingList,
    SourceEnv,
    TranslationConfigDetails,
)

from bqms_run.gcp.bqms.translation_type import TranslationType

logger = logging.getLogger(__name__)


def build(
    gcs_source_path: str,
    gcs_target_path: str,
    project: str,
    location: str,
    translation_type: TranslationType,
    source_env: Optional[SourceEnv] = None,
    object_name_mapping_list: Optional[ObjectNameMappingList] = None,
) -> CreateMigrationWorkflowRequest:
    """Builds a BQMS migration workflow request.

    Args:
        gcs_source_path: A string representing the GCS path where the input to
            be translated is located.
        gcs_target_path: A string representing the GCS path where the translated
            output will be written to.
        project: A string representing the GCS project to use.
        location: A string representing the GCP location within which to run the
            translation.
        translation_type: Abqms_run.gcp.bqms.translation_type.TranslationType.
        source_env: An optional google.cloud.bigquery_migration_v2.SourceEnv.
        object_name_mapping_list: An optional
            google.cloud.bigquery_migration_v2.ObjectNameMappingList.

    Returns:
        A google.cloud.bigquery_migration_v2.CreateMigrationWorkflowRequest.
    """
    return CreateMigrationWorkflowRequest(
        parent=f"projects/{project}/locations/{location}",
        migration_workflow=MigrationWorkflow(
            display_name=(
                "-".join(
                    [
                        translation_type.name,
                        datetime.now().strftime("%Y-%m-%d-%H:%M:%S"),
                    ]
                )
            ),
            tasks={
                "translation-task": MigrationTask(
                    type=translation_type.name,
                    translation_config_details=TranslationConfigDetails(
                        gcs_source_path=gcs_source_path,
                        gcs_target_path=gcs_target_path,
                        source_dialect=translation_type.source_dialect,
                        target_dialect=translation_type.target_dialect,
                        source_env=source_env,
                        name_mapping_list=object_name_mapping_list,
                        request_source="python-client"
                    ),
                )
            },
        ),
    )


def execute(request: CreateMigrationWorkflowRequest) -> None:
    """Execute a BQMS migration workflow request.

    Args:
        request: A
            google.cloud.bigquery_migration_v2.CreateMigrationWorkflowRequest to
            execute.
    """
    start_time = time.time()

    client = MigrationServiceClient()

    logger.info("Create migration workflow request:\n%s", request)
    response = client.create_migration_workflow(request=request)
    logger.debug("Create migration workflow response:\n%s", response)
    workflow_name: str = response.name
    workflow_name_parts = workflow_name.split("/")
    project = workflow_name_parts[1]
    location = workflow_name_parts[3]
    workflow_id = workflow_name_parts[5]

    logger.info("Polling for migration workflow status.")
    while True:
        time.sleep(5)
        processing_seconds = int(time.time() - start_time)
        request = GetMigrationWorkflowRequest(
            name=workflow_name,
        )
        workflow_status = client.get_migration_workflow(request=request)
        logger.info("Status: %s.", workflow_status.state)
        logger.info("Processing time: %s seconds.", processing_seconds)
        if workflow_status.state in (
            MigrationWorkflow.State.COMPLETED,
            MigrationWorkflow.State.PAUSED,
        ):
            break

    logger.info("Completed migration workflow in %d seconds.", processing_seconds)
    logger.info(
        "Migration workflow details: %s.",
        "https://console.cloud.google.com/bigquery/migrations/batch-translation"
        f";viewTranslationDetails={location},{workflow_id}?project={project}",
    )
