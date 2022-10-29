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
"""Performance tests."""
import cProfile
import io
import logging
import os
import pstats
import random
import shutil
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from pathlib import Path
from unittest import mock

import pytest
from google.cloud.storage import Client

from bqms_run.main import main

logger = logging.getLogger(__name__)


@pytest.fixture
def local_path(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("project")
    yield tmp_path
    shutil.rmtree(str(tmp_path))


_thread_local = threading.local()


def gcs_client():
    if getattr(_thread_local, "client", None) is None:
        _thread_local.client = Client(project=os.getenv("BQMS_PROJECT"))
    client = _thread_local.client
    return client


@pytest.fixture
def gcs_bucket():
    return os.getenv("BQMS_GCS_BUCKET")


@pytest.fixture
def gcs_prefix():
    current_unix_timestamp = int(time.time() * 1000)
    random_lowercase_string = "".join(
        random.choices(string.ascii_lowercase + string.digits, k=13)
    )
    return f"{current_unix_timestamp}-{random_lowercase_string}"


@pytest.fixture(params=[10, 10**4, 10**5])
def number_of_files(request, pytestconfig):
    if not pytestconfig.getoption("performance", default=False) and request.param != 10:
        pytest.skip()
    return request.param


def _random_sql_comment():
    return (
        "-- "
        f"{''.join(random.choices(string.ascii_letters + string.digits, k=10**2))}"
        "\n"
    )


_INPUT_FILE_SQL = (
    "create table test (a integer);\nselect * from test where a = ${foo};\n"
    f"{''.join((_random_sql_comment() for _ in range(10**2)))}"
)
_CONFIG_FILE_YAML = "translation_type: Translation_Teradata2BQ\nlocation: us\n"
_MACRO_MAPPING_FILE_YAML = "macros:\n  '*.sql':\n    '${foo}': '1'"
_OBJECT_NAME_MAPPING_FILE_JSON = """
{
  "name_map": [
    {
      "source": {
        "type": "RELATION",
        "database": "__DEFAULT_DATABASE__",
        "schema": "__DEFAULT_SCHEMA__",
        "relation": "test"
      },
      "target": {
        "database": "bq_project",
        "schema": "bq_dataset",
        "relation": "test_foo"
      }
    }
  ]
}
"""


def _write_local_config_file(config_path, config_contents):
    logger.debug("Writing config file: %s", config_path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w", encoding="utf-8") as config_file:
        config_file.write(config_contents)
    return config_path


def _write_gcs_config_file(
    gcs_bucket, config_path, config_contents
):  # pylint: disable=redefined-outer-name
    logger.debug("Writing config file: gs://%s/%s", gcs_bucket, config_path)
    client = gcs_client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(config_path)
    blob.upload_from_string(config_contents)


def _input_file_path(input_path, i):
    random_sub_path = Path(
        "/".join((str(i) for i in range(int(random.random() * 6) + 1)))
    )
    return input_path / random_sub_path / f"test_{i:06}.sql"


def _write_local_input_file(input_path, i):
    """Generate a ~10 KB file."""
    input_file_path = _input_file_path(input_path, i)
    logging.debug("Writing input file: %s", input_file_path)
    input_file_path.parent.mkdir(parents=True, exist_ok=True)
    with input_file_path.open("w", encoding="utf-8") as input_file:
        input_file.write(_INPUT_FILE_SQL)


def _write_gcs_input_file(
    gcs_bucket, input_path, i
):  # pylint: disable=redefined-outer-name
    input_file_path = _input_file_path(input_path, i)
    logging.debug("Writing input file: gs://%s/%s", gcs_bucket, input_file_path)
    client = gcs_client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(str(input_file_path))
    blob.upload_from_string(_INPUT_FILE_SQL)


def _write_input_files(
    write_fn, input_path, number_of_files
):  # pylint: disable=redefined-outer-name
    with ThreadPoolExecutor() as executor:
        futures = []

        for i in range(number_of_files):
            futures.append(executor.submit(write_fn, input_path, i))

        # Trigger any exceptions.
        for future in as_completed(futures):
            future.result()
        futures.clear()


def _profile_main():
    profile = cProfile.Profile()
    profile.enable()

    logger.info("Running main.")
    main()

    profile.disable()
    stream = io.StringIO()
    stats = pstats.Stats(profile, stream=stream)
    stats.sort_stats("tottime").print_stats(50)
    stats.sort_stats("cumtime").print_stats("cloudpathlib")
    logging.info("cProfile stats:")
    logging.info(stream.getvalue())


def test_teradata_local(
    local_path, gcs_bucket, gcs_prefix, number_of_files
):  # pylint: disable=redefined-outer-name
    """Local performance test."""
    config_path = local_path / "config/config.yaml"
    _write_local_config_file(config_path, _CONFIG_FILE_YAML)

    macro_mapping_path = local_path / "config/macros_mapping.yaml"
    _write_local_config_file(macro_mapping_path, _MACRO_MAPPING_FILE_YAML)

    object_name_mapping_path = local_path / "config/object_name_mapping.json"
    _write_local_config_file(object_name_mapping_path, _OBJECT_NAME_MAPPING_FILE_JSON)

    input_path = local_path / "input"
    _write_input_files(_write_local_input_file, input_path, number_of_files)

    with mock.patch.dict(
        os.environ,
        {
            "BQMS_INPUT_PATH": str(input_path),
            "BQMS_PREPROCESSED_PATH": f"gs://{gcs_bucket}/{gcs_prefix}/preprocessed",
            "BQMS_TRANSLATED_PATH": f"gs://{gcs_bucket}/{gcs_prefix}/translated",
            "BQMS_POSTPROCESSED_PATH": str(local_path / "postprocessed"),
            "BQMS_CONFIG_PATH": str(config_path),
            "BQMS_MACRO_MAPPING_PATH": str(macro_mapping_path),
            "BQMS_OBJECT_NAME_MAPPING_PATH": str(object_name_mapping_path),
        },
    ):
        _profile_main()


def test_teradata_gcs(
    gcs_bucket, gcs_prefix, number_of_files
):  # pylint: disable=redefined-outer-name
    """GCS performance test."""
    config_path = f"{gcs_prefix}/config/config.yaml"
    _write_gcs_config_file(gcs_bucket, config_path, _CONFIG_FILE_YAML)

    macro_mapping_path = f"{gcs_prefix}/config/macros_mapping.yaml"
    _write_gcs_config_file(gcs_bucket, macro_mapping_path, _MACRO_MAPPING_FILE_YAML)

    object_name_mapping_path = f"{gcs_prefix}/config/object_name_mapping.json"
    _write_gcs_config_file(
        gcs_bucket, object_name_mapping_path, _OBJECT_NAME_MAPPING_FILE_JSON
    )

    input_path = f"{gcs_prefix}/input"
    _write_input_files(
        partial(_write_gcs_input_file, gcs_bucket), input_path, number_of_files
    )

    with mock.patch.dict(
        os.environ,
        {
            "BQMS_INPUT_PATH": f"gs://{gcs_bucket}/{input_path}",
            "BQMS_PREPROCESSED_PATH": f"gs://{gcs_bucket}/{gcs_prefix}/preprocessed",
            "BQMS_TRANSLATED_PATH": f"gs://{gcs_bucket}/{gcs_prefix}/translated",
            "BQMS_POSTPROCESSED_PATH": f"gs://{gcs_bucket}/{gcs_prefix}/postprocessed",
            "BQMS_CONFIG_PATH": f"gs://{gcs_bucket}/{config_path}",
            "BQMS_MACRO_MAPPING_PATH": f"gs://{gcs_bucket}/{macro_mapping_path}",
            "BQMS_OBJECT_NAME_MAPPING_PATH": (
                f"gs://{gcs_bucket}/{object_name_mapping_path}"
            ),
        },
    ):
        _profile_main()
