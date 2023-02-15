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
"""The main entrypoint of the BQMS Python Client."""

import json
import logging
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from pprint import pformat
from types import FrameType
from typing import Dict, Mapping, Optional

import yaml
from cloudpathlib.cloudpath import register_path_class
from google.cloud.bigquery_migration_v2 import ObjectNameMappingList, SourceEnv
from marshmallow import ValidationError

from bqms_run import workflow
from bqms_run.encoding import EncodingDetector
from bqms_run.gcp.bqms.object_name_mapping import ObjectNameMappingListSchema
from bqms_run.gcp.bqms.request import build as build_bqms_request
from bqms_run.gcp.bqms.source_env import SourceEnvSchema
from bqms_run.gcp.bqms.translation_type import TranslationType
from bqms_run.gcp.gcs import GSClient, GSPath
from bqms_run.hooks import custom_macros
from bqms_run.hooks import postprocess as postprocess_hook
from bqms_run.hooks import preprocess as preprocess_hook
from bqms_run.macros import MacroExpanderRouter, MacroMapping
from bqms_run.paths import Path, Paths

root_logger = logging.getLogger()
logger = logging.getLogger("bqms_run")
# urllib3 can be noisy
logging.getLogger("urllib3").setLevel(logging.ERROR)


def _parse_paths() -> Paths:
    logger.debug("Parsing BQMS_*_PATH env vars.")
    paths_mapping = {
        "input_path": os.getenv("BQMS_INPUT_PATH"),
        "preprocessed_path": os.getenv("BQMS_PREPROCESSED_PATH"),
        "translated_path": os.getenv("BQMS_TRANSLATED_PATH"),
        "postprocessed_path": os.getenv("BQMS_POSTPROCESSED_PATH"),
        "config_path": os.getenv("BQMS_CONFIG_PATH"),
    }

    macro_mapping_path = os.getenv("BQMS_MACRO_MAPPING_PATH")
    if macro_mapping_path:
        paths_mapping["macro_mapping_path"] = macro_mapping_path

    object_name_mapping_path = os.getenv("BQMS_OBJECT_NAME_MAPPING_PATH")
    if object_name_mapping_path:
        paths_mapping["object_name_mapping_path"] = object_name_mapping_path

    try:
        paths = Paths.from_mapping(paths_mapping)
    except ValidationError as errors:
        for path_name, path_errors in errors.messages_dict.items():
            for path_error in path_errors:
                logger.error("Invalid BQMS_%s: %s", path_name.upper(), path_error)
        sys.exit(1)
    for path_name, path in paths:
        logger.debug("%s: %s.", path_name.capitalize(), path)
    return paths


def _read_config(config_path: Path) -> Dict[str, object]:
    logger.debug("Parsing config: %s.", config_path.as_uri())
    with config_path.open(mode="rb") as config_file:
        config_text = EncodingDetector().decode(config_file.read())
    config: Dict[str, object] = yaml.load(config_text, Loader=yaml.SafeLoader)
    return config


def _parse_location(config: Mapping[str, object]) -> str:
    logger.debug("Parsing location.")
    location = config.get("location")
    if not location:
        logger.error("location must be set.")
        sys.exit(1)
    logger.debug("Region: %s.", location)
    return str(location)


def _parse_translation_type(config: Mapping[str, object]) -> TranslationType:
    logger.debug("Parsing translation_type.")
    try:
        validated_translation_type = TranslationType.from_mapping(
            {
                "name": config.get("translation_type"),
            }
        )
    except ValidationError as errors:
        for error in errors.messages_dict["name"]:
            logger.error("Invalid translation_type: %s", error)
        sys.exit(1)
    logger.debug("Translation type: %s.", validated_translation_type.name)
    return validated_translation_type


def _parse_source_env(config: Mapping[str, object]) -> Optional[SourceEnv]:
    logger.debug("Parsing source env config.")
    source_env = None
    source_env_mapping: Dict[str, object] = {}

    source_env_default_database = config.get("default_database")
    if source_env_default_database:
        source_env_mapping["default_database"] = source_env_default_database

    source_env_schema_search_path = config.get("schema_search_path")
    if source_env_schema_search_path:
        source_env_mapping["schema_search_path"] = source_env_schema_search_path

    if source_env_mapping:
        try:
            source_env = SourceEnvSchema.from_mapping(source_env_mapping)
        except ValidationError as errors:
            for source_env_attr, source_env_attr_errors in errors.messages_dict.items():
                for source_env_attr_error in source_env_attr_errors:
                    logger.error(
                        "Invalid source env config: %s: %s",
                        source_env_attr,
                        source_env_attr_error,
                    )
            sys.exit(1)
        logger.debug("Source env default database: %s.", source_env.default_database)
        logger.debug(
            "Source env schema search path: %s.", source_env.schema_search_path
        )

    return source_env


def _parse_object_name_mapping(
    object_name_mapping_path: Path,
) -> ObjectNameMappingList:
    logger.debug("Parsing object name mapping: %s.", object_name_mapping_path.as_uri())
    with object_name_mapping_path.open(mode="rb") as macro_mapping_file:
        object_name_mapping_text = EncodingDetector().decode(macro_mapping_file.read())
    object_name_mapping = json.loads(object_name_mapping_text)
    try:
        object_name_mapping_list = ObjectNameMappingListSchema.from_mapping(
            object_name_mapping
        )
    except ValidationError as error:
        logger.error(
            "Invalid object name mapping: %s: %s",
            object_name_mapping_path.as_uri(),
            error,
        )
        sys.exit(1)
    logger.debug(
        "Object name mapping:\n%s",
        pformat(object_name_mapping_list),
    )
    return object_name_mapping_list


def _parse_macro_mapping(macro_mapping_path: Path) -> MacroExpanderRouter:
    logger.debug("Parsing macro mapping: %s.", macro_mapping_path.as_uri())
    with macro_mapping_path.open(mode="rb") as macro_mapping_file:
        macro_mapping_text = EncodingDetector().decode(macro_mapping_file.read())
    macro_mapping = yaml.load(macro_mapping_text, Loader=yaml.SafeLoader)
    try:
        validated_macro_mapping = MacroMapping.from_mapping(macro_mapping)
    except ValidationError as error:
        logger.error(
            "Invalid macro mapping: %s: %s", macro_mapping_path.as_uri(), error
        )
        sys.exit(1)
    # Log macro mapping at INFO level since it is not logged as part of
    # migration workflow request.
    logger.info(
        "Macro mapping:\n%s.",
        pformat(validated_macro_mapping),
    )
    return custom_macros(validated_macro_mapping)


class LoggingFormatter(logging.Formatter):
    """Color-coded logging.Formatter."""

    red = "\033[0;31m"
    light_red = "\033[0;91m"
    yellow = "\033[0;33m"
    green = "\033[0;32m"
    white = "\033[0;97m"
    reset = "\033[0m"

    format_string = (
        "%(asctime)s: {color}%(levelname)s:{reset} %(threadName)s: %(message)s"
    )

    formats = {
        logging.DEBUG: format_string.format(color=white, reset=reset),
        logging.INFO: format_string.format(color=green, reset=reset),
        logging.WARNING: format_string.format(color=yellow, reset=reset),
        logging.ERROR: format_string.format(color=light_red, reset=reset),
        logging.CRITICAL: format_string.format(color=red, reset=reset),
    }

    def format(self, record: logging.LogRecord) -> str:
        log_fmt = self.formats.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def _shutdown_handler(  # pylint: disable=unused-argument,redefined-outer-name
    signal: int, frame: Optional[FrameType] = None
) -> None:
    logger.info("Signal received, safely shutting down.")
    sys.exit(0)


signal.signal(signal.SIGTERM, _shutdown_handler)


def main() -> None:
    """Parse settings, instantiate object graph and run translation workflow."""
    true_env_var_values = ("true", "1", "t")

    verbose = os.getenv("BQMS_VERBOSE", "False").lower() in true_env_var_values
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    if os.getenv("BQMS_CLOUD_LOGGING", "False").lower() in true_env_var_values:
        # TODO: The Cloud Logging Python library currently only detects Cloud
        #       Run services not Cloud Run jobs. This confusingly results in
        #       logs not being properly associated with the cloud_run_job
        #       resource. Until this is fixed, log to stdout which will be
        #       properly associated with the cloud_run_job resource. See:
        #       https://github.com/googleapis/python-logging/blob/8a67b73cdfcb9da545671be6cf59c724360b1544/google/cloud/logging_v2/handlers/_monitored_resources.py#L26-L34  # pylint: disable=line-too-long
        #       https://cloud.google.com/logging/docs/api/platform-logs#cloud_run.
        # cloud_logging_handler = google.cloud.logging.Client().get_default_handler()
        # logger.addHandler(cloud_logging_handler)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter("%(levelname)s: %(threadName)s: %(message)s")
        )
        root_logger.addHandler(console_handler)
    else:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(LoggingFormatter())
        root_logger.addHandler(console_handler)

    logger.info("Parsing env vars and config files.")

    logger.debug("Parsing BQMS_MULTITHREADED.")
    multithreaded = (
        os.getenv("BQMS_MULTITHREADED", "False").lower() in true_env_var_values
    )
    logger.debug("Multithreaded: %s.", multithreaded)

    logger.debug("Parsing BQMS_PROJECT.")
    project = os.getenv("BQMS_PROJECT")
    if not project:
        logger.error("BQMS_PROJECT must be set.")
        sys.exit(1)
    logger.debug("Project: %s.", project)

    gcs_client = GSClient(project)
    gcs_client.set_as_default_client()

    if os.getenv("BQMS_GCS_CHECKS", "False").lower() not in true_env_var_values:
        register_path_class("gs")(GSPath)

    paths = _parse_paths()

    config = _read_config(paths.config_path)
    location = _parse_location(config)
    translation_type = _parse_translation_type(config)
    source_env = _parse_source_env(config)

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

    macro_expander_router = (
        _parse_macro_mapping(paths.macro_mapping_path)
        if paths.macro_mapping_path
        else None
    )

    try:
        logger.info("Executing BQMS workflow.")
        workflow.execute(
            paths,
            preprocess_hook,
            postprocess_hook,
            bqms_request,
            macro_expander_router,
            ThreadPoolExecutor if multithreaded else workflow.SynchronousExecutor,
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("An unexpected error occurred:")
        raise exc


if __name__ == "__main__":
    main()
