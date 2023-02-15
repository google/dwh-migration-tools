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
import logging
import os
import random
import shutil
import string
import threading
import time
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


_CONFIG_FILE_YAML = "translation_type: Translation_Bteq2BQ\nlocation: us\n"
_INPUT_FILE_UTF8_KSH = """
#!/bin/ksh
## A Very simple test.
bteq  <<EOF
SELECT 123, 'foo', '🃏', 456, '☕' from bar;
EOF
echo Trying another select.
"""
_INPUT_FILE_ISO_8859_1_KSH = """
#!/bin/ksh
## A Very simple test.
bteq  <<EOF
SELECT 'ä,ö,ü' from baz;
EOF
echo Trying another select.
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


def _write_local_input_utf8_file(input_path):
    input_file_path = input_path / "test_utf8.ksh"
    logging.debug("Writing input file: %s", input_file_path)
    input_file_path.parent.mkdir(parents=True, exist_ok=True)
    with input_file_path.open("w", encoding="utf-8") as input_file:
        input_file.write(_INPUT_FILE_UTF8_KSH)


def _write_local_input_iso_8859_1_file(input_path):
    input_file_path = input_path / "test_iso_8859_1.ksh"
    logging.debug("Writing input file: %s", input_file_path)
    input_file_path.parent.mkdir(parents=True, exist_ok=True)
    with input_file_path.open("w", encoding="iso-8859-1") as input_file:
        input_file.write(_INPUT_FILE_ISO_8859_1_KSH)


def _write_gcs_input_utf8_file(
    gcs_bucket, input_path
):  # pylint: disable=redefined-outer-name
    input_file_path = input_path / "test_utf8.ksh"
    logging.debug("Writing input file: gs://%s/%s", gcs_bucket, input_file_path)
    client = gcs_client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(str(input_file_path))
    blob.upload_from_string(_INPUT_FILE_UTF8_KSH)


def _write_gcs_input_iso_8859_1_file(
    gcs_bucket, input_path
):  # pylint: disable=redefined-outer-name
    input_file_path = input_path / "test_iso_8859_1.ksh"
    logging.debug("Writing input file: gs://%s/%s", gcs_bucket, input_file_path)
    client = gcs_client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(str(input_file_path))
    blob.upload_from_string(_INPUT_FILE_ISO_8859_1_KSH)


def test_bteq_local(
    local_path, gcs_bucket, gcs_prefix
):  # pylint: disable=redefined-outer-name
    """
    Local integration test exercising KSH/heredoc SQL extractor.
    """
    config_path = local_path / "config/config.yaml"
    _write_local_config_file(config_path, _CONFIG_FILE_YAML)

    input_path = local_path / "input"
    _write_local_input_utf8_file(input_path)
    _write_local_input_iso_8859_1_file(input_path)

    with mock.patch.dict(
        os.environ,
        {
            "BQMS_INPUT_PATH": str(input_path),
            "BQMS_PREPROCESSED_PATH": f"gs://{gcs_bucket}/{gcs_prefix}/preprocessed",
            "BQMS_TRANSLATED_PATH": f"gs://{gcs_bucket}/{gcs_prefix}/translated",
            "BQMS_POSTPROCESSED_PATH": str(local_path / "postprocessed"),
            "BQMS_CONFIG_PATH": str(config_path),
        },
    ):
        logger.info("Running main.")
        main()


def test_bteq_gcs(gcs_bucket, gcs_prefix):  # pylint: disable=redefined-outer-name
    """
    GCS integration test exercising KSH/heredoc SQL extractor.
    """
    config_path = f"{gcs_prefix}/config/config.yaml"
    _write_gcs_config_file(gcs_bucket, config_path, _CONFIG_FILE_YAML)

    input_path = Path(f"{gcs_prefix}/input")
    _write_gcs_input_utf8_file(gcs_bucket, input_path)
    _write_gcs_input_iso_8859_1_file(gcs_bucket, input_path)

    with mock.patch.dict(
        os.environ,
        {
            "BQMS_INPUT_PATH": f"gs://{gcs_bucket}/{input_path}",
            "BQMS_PREPROCESSED_PATH": f"gs://{gcs_bucket}/{gcs_prefix}/preprocessed",
            "BQMS_TRANSLATED_PATH": f"gs://{gcs_bucket}/{gcs_prefix}/translated",
            "BQMS_POSTPROCESSED_PATH": f"gs://{gcs_bucket}/{gcs_prefix}/postprocessed",
            "BQMS_CONFIG_PATH": f"gs://{gcs_bucket}/{config_path}",
        },
    ):
        logger.info("Running main.")
        main()
