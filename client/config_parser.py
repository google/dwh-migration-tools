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

import yaml
import os

from os.path import abspath, exists, isfile, join, dirname
from yaml.loader import SafeLoader

TERADATA2BQ = "Translation_Teradata2BQ"
REDSHIFT2BQ = "Translation_Redshift2BQ"
BTEQ2BQ = "Translation_Bteq2BQ"
ORACLE2BQ = "Translation_Oracle2BQ"
HIVEQL2BQ = "Translation_HiveQL2BQ"
SPARKSQL2BQ = "Translation_SparkSQL2BQ"
SNOWFLAKE2BQ = "Translation_Snowflake2BQ"
NETEZZA2BQ = "Translation_Netezza2BQ"

REPO_ROOT_DIR = "dwh-migration-tools"
CLIENT_DIR = "client"


class TranslationConfig:
    """A structure for holding the config info of the translation job.
    """

    def __init__(self):
        self.project_number = None
        self.gcs_bucket = None
        self.location = None
        self.translation_type = None
        self.input_directory = None
        self.output_directory = None
        self.macro_maps = None
        self.output_token_maps = None
        self.clean_up_tmp_files = True


class ConfigParser:
    """A parser for the config file.
    """

    # Config field name
    __TRANSLATION_TYPE = "translation_type"
    __TRANSLATION_CONFIG = "translation_config"
    __INPUT_DIR = "input_directory"
    __OUTPUT_DIR = "output_directory"
    __OUTPUT_TOKEN_MAPS = "output_token_replacement_maps"
    __CLEAN_UP = "clean_up_tmp_files"

    # Config default values
    __DEFAULT_INPUT_DIR = "input"
    __DEFAULT_OUTPUT_DIR = "output"

    # The supported task types
    __SUPPORTED_TYPES = {
        TERADATA2BQ,
        REDSHIFT2BQ,
        BTEQ2BQ,
        ORACLE2BQ,
        HIVEQL2BQ,
        SPARKSQL2BQ,
        SNOWFLAKE2BQ,
        NETEZZA2BQ,
    }

    def parse_config(self, config_file: str = 'config.yaml') -> TranslationConfig:
        """Parses the config file into TranslationConfig.

        Args:
            config_file: path to the config file.  Default value is config.yaml.
        Return:
            translation config.
        """
        with open(validate_path(config_file)) as f:
            data = yaml.load(f, Loader=SafeLoader)
        self.validate_config_yaml(data)

        config = TranslationConfig()

        gcp_settings_input = data["gcp_settings"]
        config.project_number = gcp_settings_input["project_number"]
        config.gcs_bucket = gcp_settings_input["gcs_bucket"]

        translation_config_input = data[self.__TRANSLATION_CONFIG]
        config.location = translation_config_input["location"]
        config.translation_type = translation_config_input[self.__TRANSLATION_TYPE]
        input_directory = self.__DEFAULT_INPUT_DIR if self.__INPUT_DIR not in translation_config_input \
            else translation_config_input[self.__INPUT_DIR]
        config.input_directory = validate_path(input_directory)
        config.output_directory = self.__DEFAULT_OUTPUT_DIR if self.__OUTPUT_DIR not in translation_config_input \
            else translation_config_input[self.__OUTPUT_DIR]
        if os.getcwd().endswith(REPO_ROOT_DIR) and config.output_directory == self.__DEFAULT_OUTPUT_DIR:
            config.output_directory = join(CLIENT_DIR, self.__DEFAULT_OUTPUT_DIR)

        config.clean_up_tmp_files = True if self.__CLEAN_UP not in translation_config_input \
            else translation_config_input[self.__CLEAN_UP]

        if not os.path.exists(config.output_directory):
            os.makedirs(config.output_directory)

        print("Finished Parsing translation config: ")
        print('\n'.join("     %s: %s" % item for item in vars(config).items()))
        return config

    def validate_config_yaml(self, yaml_data):
        """Validate the data in the config yaml file.
        """
        assert self.__TRANSLATION_CONFIG in yaml_data, "Missing translation_config field in config.yaml."
        assert self.__TRANSLATION_TYPE in yaml_data[
            self.__TRANSLATION_CONFIG], "Missing translation_type field in config.yaml."
        type = yaml_data[self.__TRANSLATION_CONFIG][self.__TRANSLATION_TYPE]
        assert type in self.__SUPPORTED_TYPES, "The type \"%s\" is not supported." % type


def validate_path(file_path: str) -> str:
    """Validates the path exists. Returns the correct path if the validation fails because the user runs the
    tool from the root dir.
    """
    if exists(file_path):
        return file_path
    elif os.getcwd().endswith(REPO_ROOT_DIR) and exists(join(CLIENT_DIR, file_path)):
        return join(CLIENT_DIR, file_path)
    else:
        raise ValueError("The path \"%s\" don't exist." % file_path)


def validate_dir():
    """Validates that the user runs under either the repo root or under the client directory.
    """
    current_path = os.getcwd()
    assert current_path.endswith(REPO_ROOT_DIR) or current_path.endswith(join(REPO_ROOT_DIR, CLIENT_DIR)), \
        "You need to run the client tool under the directory of \"%s\" or \"%s\". The current path is \"%s\"" %\
        (REPO_ROOT_DIR, join(REPO_ROOT_DIR, CLIENT_DIR), current_path)
