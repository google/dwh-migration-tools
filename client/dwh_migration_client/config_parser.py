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
"""A parser for the config file."""

import logging
from dataclasses import asdict, dataclass
from os.path import abspath
from pprint import pformat
from typing import Dict, Optional, Tuple

import yaml
from yaml.loader import SafeLoader

AZURESYNAPSE2BQ = "Translation_AzureSynapse2BQ"
BTEQ2BQ = "Translation_Bteq2BQ"
HIVEQL2BQ = "Translation_HiveQL2BQ"
NETEZZA2BQ = "Translation_Netezza2BQ"
ORACLE2BQ = "Translation_Oracle2BQ"
REDSHIFT2BQ = "Translation_Redshift2BQ"
SNOWFLAKE2BQ = "Translation_Snowflake2BQ"
SPARKSQL2BQ = "Translation_SparkSQL2BQ"
TERADATA2BQ = "Translation_Teradata2BQ"
VERTICA2BQ = "Translation_Vertica2BQ"
SQLSERVER2BQ = "Translation_SQLServer2BQ"


@dataclass
class TranslationConfig:
    """A structure for holding the config info of the translation job."""

    project_number: str
    gcs_bucket: str
    location: str
    translation_type: str
    default_database: Optional[str] = None
    schema_search_path: Optional[Tuple[str]] = None
    clean_up_tmp_files: bool = True


class ConfigParser:  # pylint: disable=too-few-public-methods
    """A parser for the config file."""

    def __init__(self, config_file_path: str) -> None:
        self._config_file_path = abspath(config_file_path)

    # Config field name
    _TRANSLATION_TYPE = "translation_type"
    _TRANSLATION_CONFIG = "translation_config"
    _DEFAULT_DATABASE = "default_database"
    _SCHEMA_SEARCH_PATH = "schema_search_path"
    _CLEAN_UP = "clean_up_tmp_files"

    # The supported task types
    _SUPPORTED_TYPES = {
        AZURESYNAPSE2BQ,
        BTEQ2BQ,
        HIVEQL2BQ,
        NETEZZA2BQ,
        ORACLE2BQ,
        REDSHIFT2BQ,
        SNOWFLAKE2BQ,
        SPARKSQL2BQ,
        TERADATA2BQ,
        VERTICA2BQ,
        SQLSERVER2BQ,
    }

    def parse_config(self) -> TranslationConfig:
        """Parses the config file into TranslationConfig.

        Return:
            translation config.
        """
        logging.info(
            "Reading translation config file from %s...", self._config_file_path
        )
        with open(self._config_file_path, encoding="utf-8") as file:
            data = yaml.load(file, Loader=SafeLoader)
        self._validate_config_yaml(data)

        gcp_settings_input = data["gcp_settings"]
        project_number = gcp_settings_input["project_number"]
        gcs_bucket = gcp_settings_input["gcs_bucket"]

        translation_config_input = data[self._TRANSLATION_CONFIG]
        location = translation_config_input["location"]
        translation_type = translation_config_input[self._TRANSLATION_TYPE]

        clean_up_tmp_files = (
            True
            if self._CLEAN_UP not in translation_config_input
            else translation_config_input[self._CLEAN_UP]
        )

        default_database = translation_config_input.get(self._DEFAULT_DATABASE)
        schema_search_path = translation_config_input.get(self._SCHEMA_SEARCH_PATH)

        config = TranslationConfig(
            project_number=project_number,
            gcs_bucket=gcs_bucket,
            location=location,
            translation_type=translation_type,
            clean_up_tmp_files=clean_up_tmp_files,
            default_database=default_database,
            schema_search_path=schema_search_path,
        )

        logging.info("Finished parsing translation config.")
        logging.info("The config is:\n%s", pformat(asdict(config)))
        return config

    def _validate_config_yaml(self, yaml_data: Dict[str, Dict[str, str]]) -> None:
        """Validate the data in the config yaml file."""
        assert (
            self._TRANSLATION_CONFIG in yaml_data
        ), "Missing translation_config field in config.yaml."
        assert (
            self._TRANSLATION_TYPE in yaml_data[self._TRANSLATION_CONFIG]
        ), "Missing translation_type field in config.yaml."
        translation_type = yaml_data[self._TRANSLATION_CONFIG][self._TRANSLATION_TYPE]
        assert (
            translation_type in self._SUPPORTED_TYPES
        ), f'The type "{translation_type}" is not supported.'
