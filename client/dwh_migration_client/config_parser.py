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

from dataclasses import dataclass
from os.path import abspath
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
    __TRANSLATION_TYPE = "translation_type"
    __TRANSLATION_CONFIG = "translation_config"
    __DEFAULT_DATABASE = "default_database"
    __SCHEMA_SEARCH_PATH = "schema_search_path"
    __CLEAN_UP = "clean_up_tmp_files"

    # The supported task types
    __SUPPORTED_TYPES = {
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
        print("Reading translation config file from %s..." % self._config_file_path)
        with open(self._config_file_path, encoding="utf-8") as file:
            data = yaml.load(file, Loader=SafeLoader)
        self.__validate_config_yaml(data)

        gcp_settings_input = data["gcp_settings"]
        project_number = gcp_settings_input["project_number"]
        gcs_bucket = gcp_settings_input["gcs_bucket"]

        translation_config_input = data[self.__TRANSLATION_CONFIG]
        location = translation_config_input["location"]
        translation_type = translation_config_input[self.__TRANSLATION_TYPE]

        clean_up_tmp_files = (
            True
            if self.__CLEAN_UP not in translation_config_input
            else translation_config_input[self.__CLEAN_UP]
        )

        default_database = translation_config_input.get(self.__DEFAULT_DATABASE)
        schema_search_path = translation_config_input.get(self.__SCHEMA_SEARCH_PATH)

        config = TranslationConfig(
            project_number=project_number,
            gcs_bucket=gcs_bucket,
            location=location,
            translation_type=translation_type,
            clean_up_tmp_files=clean_up_tmp_files,
            default_database=default_database,
            schema_search_path=schema_search_path,
        )

        print("Finished parsing translation config.")
        print("The config is:")
        print("\n".join("     %s: %s" % item for item in vars(config).items()))
        return config

    def __validate_config_yaml(self, yaml_data: Dict[str, Dict[str, str]]) -> None:
        """Validate the data in the config yaml file."""
        assert (
            self.__TRANSLATION_CONFIG in yaml_data
        ), "Missing translation_config field in config.yaml."
        assert (
            self.__TRANSLATION_TYPE in yaml_data[self.__TRANSLATION_CONFIG]
        ), "Missing translation_type field in config.yaml."
        translation_type = yaml_data[self.__TRANSLATION_CONFIG][self.__TRANSLATION_TYPE]
        assert translation_type in self.__SUPPORTED_TYPES, (
            'The type "%s" is not supported.' % translation_type
        )
