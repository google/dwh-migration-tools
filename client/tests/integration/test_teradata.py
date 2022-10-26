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
"""Teradata translation tests."""
import os
import random
import string
import time
from unittest import mock

from bqms_run.main import main


def test_sql():
    """Teradata SQL translation test."""
    examples_path = "examples/teradata/sql"
    current_unix_timestamp = int(time.time() * 1000)
    random_lowercase_string = "".join(
        random.choices(string.ascii_lowercase + string.digits, k=13)
    )
    gcs_prefix = f"{current_unix_timestamp}-{random_lowercase_string}"
    with mock.patch.dict(
        os.environ,
        {
            "BQMS_INPUT_PATH": f"{examples_path}/input",
            "BQMS_PREPROCESSED_PATH": (
                f"gs://{os.getenv('BQMS_GCS_BUCKET')}/{gcs_prefix}/preprocessed"
            ),
            "BQMS_TRANSLATED_PATH": (
                f"gs://{os.getenv('BQMS_GCS_BUCKET')}/{gcs_prefix}/translated"
            ),
            "BQMS_POSTPROCESSED_PATH": f"{examples_path}/postprocessed",
            "BQMS_CONFIG_PATH": f"{examples_path}/config/config.yaml",
            "BQMS_MACRO_MAPPING_PATH": f"{examples_path}/config/macros_mapping.yaml",
            "BQMS_OBJECT_NAME_MAPPING_PATH": (
                f"{examples_path}/config/object_name_mapping.json"
            ),
        },
    ):
        main()
