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
"""The main entrypoint of the BQMS Cloud Run container."""

import json
import logging
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from pprint import pformat
from types import FrameType
from typing import Optional

import yaml
from google.cloud.bigquery_migration_v2 import ObjectNameMappingList, SourceEnv
from marshmallow import ValidationError

from run import workflow
from run.gcp.bqms.object_name_mapping import ObjectNameMappingListSchema
from run.gcp.bqms.request import build as build_bqms_request
from run.gcp.bqms.source_env import SourceEnvSchema
from run.gcp.bqms.translation_type import TranslationType
from run.gcp.gcs import GSClient
from run.hooks import postprocess as postprocess_hook
from run.hooks import preprocess as preprocess_hook
from run.macro_processor import MacroProcessor
from run.paths import Path, Paths


def _parse_paths_settings() -> Paths:
    logging.info("Parsing BQMS_*_PATHS settings.")
    paths_mapping = {
        "input_path": os.getenv("BQMS_INPUT_PATH"),
        "preprocessed_path": os.getenv("BQMS_PREPROCESSED_PATH"),
        "translated_path": os.getenv("BQMS_TRANSLATED_PATH"),
        "postprocessed_path": os.getenv("BQMS_POSTPROCESSED_PATH"),
    }

    macro_mapping_path = os.getenv("BQMS_MACRO_MAPPING_PATH")
    if macro_mapping_path:
        paths_mapping["macro_mapping_path"] = macro_mapping_path

    object_name_mapping_path = os.getenv("BQMS_OBJECT_NAME_MAPPING_PATH")
    if object_name_mapping_path:
        paths_mapping["object_name_mapping_path"] = object_name_mapping_path

    try:
        paths = Paths.from_mapping(paths_mapping)
    except ValidationError as error:
        logging.error("Invalid BQMS_*_PATHS setting: %s.", error)
        sys.exit(1)
    logging.info(
        "Paths:\n%s.",
        pformat(paths),
    )
    return paths


def _parse_translation_type_setting() -> TranslationType:
    logging.info("Parsing BQMS_TRANSLATION_TYPE setting.")
    try:
        translation_type = TranslationType.from_mapping(
            {
                "name": os.getenv("BQMS_TRANSLATION_TYPE"),
            }
        )
    except ValidationError as error:
        logging.error("Invalid BQMS_TRANSLATION_TYPE setting: %s.", error)
        sys.exit(1)
    logging.info(
        "Translation type:\n%s.",
        pformat(translation_type),
    )
    return translation_type


def _parse_source_env_settings() -> Optional[SourceEnv]:
    logging.info("Parsing BQMS_SOURCE_ENV_* settings.")
    source_env = None
    source_env_mapping: dict[str, object] = {}

    source_env_default_database = os.getenv("BQMS_SOURCE_ENV_DEFAULT_DATABASE")
    if source_env_default_database:
        source_env_mapping["default_database"] = source_env_default_database

    source_env_schema_search_path = os.getenv("BQMS_SOURCE_ENV_SCHEMA_SEARCH_PATH")
    if source_env_schema_search_path:
        source_env_mapping["schema_search_path"] = source_env_schema_search_path.split(
            ","
        )

    if source_env_mapping:
        try:
            source_env = SourceEnvSchema.from_mapping(source_env_mapping)
        except ValidationError as error:
            logging.error("Invalid BQMS_SOURCE_ENV_* setting: %s.", error)
            sys.exit(1)
        logging.info(
            "Source environment:\n%s.",
            pformat(source_env),
        )

    return source_env


def _parse_object_name_mapping(
    object_name_mapping_path: Path,
) -> ObjectNameMappingList:
    logging.info("Parsing object name mapping: %s.", object_name_mapping_path.as_uri())
    with object_name_mapping_path.open(
        mode="r", encoding="utf-8"
    ) as macro_mapping_file:
        object_name_mapping_text = macro_mapping_file.read()
    object_name_mapping = json.loads(object_name_mapping_text)
    try:
        object_name_mapping_list = ObjectNameMappingListSchema.from_mapping(
            object_name_mapping
        )
    except ValidationError as error:
        logging.error(
            "Invalid object name mapping: %s: %s.",
            object_name_mapping_path.as_uri(),
            error,
        )
        sys.exit(1)
    logging.info(
        "Object name mapping: %s:\n%s",
        object_name_mapping_path.as_uri(),
        pformat(object_name_mapping_list),
    )
    return object_name_mapping_list


def _parse_macro_mapping(macro_mapping_path: Path) -> MacroProcessor:
    logging.info("Parsing macro mapping: %s.", macro_mapping_path.as_uri())
    with macro_mapping_path.open(mode="r", encoding="utf-8") as macro_mapping_file:
        macro_mapping_text = macro_mapping_file.read()
    macro_mapping = yaml.load(macro_mapping_text, Loader=yaml.SafeLoader)
    try:
        macro_processor = MacroProcessor.from_mapping(macro_mapping)
    except ValidationError as error:
        logging.error(
            "Invalid macro mapping: %s: %s.", macro_mapping_path.as_uri(), error
        )
        sys.exit(1)
    logging.info(
        "Macro mapping: %s:\n%s.",
        macro_mapping_path.as_uri(),
        macro_processor,
    )
    return macro_processor


def _shutdown_handler(  # pylint: disable=unused-argument,redefined-outer-name
    signal: int, frame: Optional[FrameType] = None
) -> None:
    logging.info("Signal received, safely shutting down.")
    sys.exit(0)


signal.signal(signal.SIGTERM, _shutdown_handler)


def main() -> None:
    """Parse settings, instantiate object graph and run translation workflow."""
    true_env_var_values = ("true", "1", "t")

    verbose = os.getenv("BQMS_VERBOSE", "False").lower() in true_env_var_values
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s: %(levelname)s: %(threadName)s: %(message)s",
    )

    logging.info("Parsing BQMS_PROJECT setting.")
    project = os.getenv("BQMS_PROJECT")
    if not project:
        logging.error("BQMS_PROJECT must be set.")
        sys.exit(1)
    logging.info("Project: %s.", project)

    gcs_client = GSClient(project)
    gcs_client.set_as_default_client()

    paths = _parse_paths_settings()

    logging.info("Parsing BQMS_TRANSLATION_REGION setting.")
    location = os.getenv("BQMS_TRANSLATION_REGION")
    if not location:
        logging.error("BQMS_TRANSLATION_REGION must be set.")
        sys.exit(1)
    logging.info("Region: %s.", location)

    translation_type = _parse_translation_type_setting()

    source_env = _parse_source_env_settings()

    object_name_mapping_list = (
        _parse_object_name_mapping(paths.object_name_mapping_path)
        if paths.object_name_mapping_path
        else None
    )

    bqms_request = build_bqms_request(
        paths.preprocessed_path.as_uri(),
        paths.translated_path.as_uri(),
        project,
        location,
        translation_type,
        source_env,
        object_name_mapping_list,
    )

    macro_processor = (
        _parse_macro_mapping(paths.macro_mapping_path)
        if paths.macro_mapping_path
        else None
    )

    logging.info("Parsing BQMS_MULTITHREADED setting.")
    multithreaded = (
        os.getenv("BQMS_MULTITHREADED", "False").lower() in true_env_var_values
    )
    logging.info("Multithreaded: %s.", multithreaded)

    try:
        workflow.execute(
            paths,
            preprocess_hook,
            postprocess_hook,
            bqms_request,
            macro_processor,
            ThreadPoolExecutor if multithreaded else workflow.SynchronousExecutor,
        )
    except Exception as exc:  # pylint: disable=broad-except
        logging.error("An unexpected error occurred:")
        raise exc


if __name__ == "__main__":
    main()
