# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import shutil
import config_parser
import gcs_util
import sys
import uuid
import time

from datetime import datetime
from os.path import dirname, join
from macro_processor import MacroProcessor
from google.cloud import bigquery_migration_v2


class BatchSqlTranslator:
    """A class to manage Batch SQL Translation job using the bigquery_migration_v2
    python client library.

    """

    def __init__(self, config, preprocessor: MacroProcessor):
        self.config = config
        self.client = bigquery_migration_v2.MigrationServiceClient()
        self.gcs_path = None
        self.preprocessor = preprocessor	# May be None

    __JOB_FINISHED_STATES = {
        bigquery_migration_v2.types.MigrationWorkflow.State.COMPLETED,
        bigquery_migration_v2.types.MigrationWorkflow.State.PAUSED
    }

    # The name of a hidden directory that stores temporary processed files if user defines a macro replacement map.
    # This directory will be deleted (by default) after the job finishes.
    __TMP_DIR_NAME = ".tmp_processed"

    def start_translation(self):
        """Waits until the workflow finishes by calling the Migration Service API every 5 seconds.

        workflow_id: the workflow id in the format of
        length_seconds: max wait time.
        """
        local_input_dir = self.config.input_directory
        local_output_dir = self.config.output_directory
        tmp_dir = join(dirname(self.config.input_directory), self.__TMP_DIR_NAME)
        if self.preprocessor is not None:
            print("Start pre-processing input query files...")
            local_input_dir = join(tmp_dir, "input")
            local_output_dir = join(tmp_dir, "output")
            self.preprocessor.pre_process(self.config.input_directory, local_input_dir)

        self.gcs_path = self.__generate_gcs_path()
        gcs_input_path = join("gs://%s" % self.config.gcs_bucket, self.gcs_path, "input")
        gcs_output_path = join("gs://%s" % self.config.gcs_bucket, self.gcs_path, "output")
        print("Uploading inputs to gcs ...")
        gcs_util.upload_directory(local_input_dir, self.config.gcs_bucket, join(self.gcs_path, "input"))
        print("Start translation job...")
        job_name = self.create_migration_workflow(gcs_input_path, gcs_output_path)
        self.__wait_until_job_finished(job_name)
        print("Downloading outputs...")
        gcs_util.download_directory(local_output_dir, self.config.gcs_bucket, join(self.gcs_path, "output"))

        if self.preprocessor is not None:
            print("Start post-processing by reverting the macros substitution...")
            self.preprocessor.post_process(local_output_dir, self.config.output_directory)

        print("Finished post-processing. The output query files are in %s" % self.config.output_directory)

        if self.config.clean_up_tmp_files:
            print("Cleaning up tmp files...")
            shutil.rmtree(tmp_dir)

        print("The job finished successfully!")
        print(
            "To view the job details, please go to the link: %s" % self.__get_ui_link())
        print("Thank you for using the python exemplary library!")

    def __generate_gcs_path(self) -> str:
        """Generates a gcs_path in the format of {translation_type}-{yyyy-mm-dd}-xxxx-xxxx-xxx-xxxx-xxxxxx.
        The suffix is a random generated uuid string.
        """
        return ("%s-%s-%s" % (self.config.translation_type, datetime.now().strftime('%Y-%m-%d')
                              , str(uuid.uuid4())))

    def __get_ui_link(self) -> str:
        """Returns the http link to the offline translation page for this project.
        """
        return ("https://console.cloud.google.com/bigquery/migrations/offline-translation?projectnumber=%s" %
                self.config.project_number)

    def __wait_until_job_finished(self, workflow_id: str, length_seconds: int = 600):
        """Waits until the workflow finishes by calling the Migration Service API every 5 seconds.

        workflow_id: the workflow id in the format of
        length_seconds: max wait time.
        """
        start_time = time.time()
        processing_seconds = 0
        while processing_seconds < length_seconds:
            time.sleep(5)
            processing_seconds = int(time.time() - start_time);
            job_status = self.get_migration_workflow(workflow_id)
            print("Translation job status is %s. Processing time: %s seconds" % (job_status.state, processing_seconds))
            if job_status.state in self.__JOB_FINISHED_STATES:
                return
        print("The job is still running after %d seconds. Please go to the UI page and download the outputs manually %s"
              % (processing_seconds, self.__get_ui_link()))
        sys.exit()

    def list_migration_workflows(self, num_jobs=5):
        """Lists the most recent bigquery migration workflows status and prints on the terminal.

        num_jobs: the number of workflows to print (default value is 5).
        """
        print("List migration workflows for project %s" % self.config.project_number)
        request = bigquery_migration_v2.ListMigrationWorkflowsRequest(
            parent="projects/%s/locations/%s" % (self.config.project_number, self.config.location),
        )

        page_result = self.client.list_migration_workflows(request=request)

        for i, response in enumerate(page_result):
            if i < num_jobs:
                print(response)

    def get_migration_workflow(self, job_name):
        """Starts a get API call for a migration workflow and print out the status on terminal.
        """
        print("Get migration workflows for %s" % job_name)
        request = bigquery_migration_v2.GetMigrationWorkflowRequest(
            name=job_name,
        )

        page_result = self.client.get_migration_workflow(request=request)
        return page_result

    def create_migration_workflow(self, gcs_input_path: str, gcs_output_path: str) -> str:
        """Creates a migration workflow and returns the name of the workflow.
        """
        target_dialect = bigquery_migration_v2.Dialect();
        target_dialect.bigquery_dialect = bigquery_migration_v2.BigQueryDialect();

        translation_config = bigquery_migration_v2.TranslationConfigDetails(
            gcs_source_path=gcs_input_path,
            gcs_target_path=gcs_output_path,
            source_dialect=self.get_input_dialect(),
            target_dialect=target_dialect
        )

        migration_task = bigquery_migration_v2.MigrationTask(
            type=self.config.translation_type,
            translation_config_details=translation_config
        )

        workflow = bigquery_migration_v2.MigrationWorkflow(
            display_name="%s-cli-%s" % (self.config.translation_type, datetime.now().strftime('%m-%d-%H:%M'))
        )

        workflow.tasks["translation-task"] = migration_task
        request = bigquery_migration_v2.CreateMigrationWorkflowRequest(
            parent="projects/%s/locations/%s" % (self.config.project_number, self.config.location),
            migration_workflow=workflow,
        )

        response = self.client.create_migration_workflow(request=request)
        print(response)
        return response.name

    def get_input_dialect(self) -> bigquery_migration_v2.Dialect:
        """Returns the input dialect proto based on the translation type in the config.
        """
        dialect = bigquery_migration_v2.Dialect()
        if self.config.translation_type == config_parser.TERADATA2BQ:
            dialect.teradata_dialect = bigquery_migration_v2.TeradataDialect(
                mode=bigquery_migration_v2.TeradataDialect.Mode.SQL)
        elif self.config.translation_type == config_parser.BTEQ2BQ:
            dialect.teradata_dialect = bigquery_migration_v2.TeradataDialect(
                mode=bigquery_migration_v2.TeradataDialect.Mode.BTEQ)
        elif self.config.translation_type == config_parser.REDSHIFT2BQ:
            dialect.redshift_dialect = bigquery_migration_v2.RedshiftDialect()
        elif self.config.translation_type == config_parser.ORACLE2BQ:
            dialect.oracle_dialect = bigquery_migration_v2.OracleDialect()
        elif self.config.translation_type == config_parser.HIVEQL2BQ:
            dialect.hiveql_dialect = bigquery_migration_v2.HiveQLDialect()
        elif self.config.translation_type == config_parser.SPARKSQL2BQ:
            dialect.sparksql_dialect = bigquery_migration_v2.SparkSQLDialect()
        elif self.config.translation_type == config_parser.SNOWFLAKE2BQ:
            dialect.snowflake_dialect = bigquery_migration_v2.SnowflakeDialect()
        elif self.config.translation_type == config_parser.NETEZZA2BQ:
            dialect.netezza_dialect = bigquery_migration_v2.NetezzaDialect()
        return dialect
