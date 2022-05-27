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

from argparse import Namespace
from os.path import exists, abspath
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

DEFAULT_CONFIG_PATH = "client/config.yaml"
DEFAULT_INPUT_PATH = "client/input"
DEFAULT_OUTPUT_PATH = "client/output"


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

    def __init__(self, argument: Namespace):
        self.__argument = argument

    # Config field name
    __TRANSLATION_TYPE = "translation_type"
    __TRANSLATION_CONFIG = "translation_config"
    __CLEAN_UP = "clean_up_tmp_files"

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

    def parse_config(self) -> TranslationConfig:
        """Parses the config file into TranslationConfig.

        Return:
            translation config.
        """
        config = TranslationConfig()
        config_file_path = DEFAULT_CONFIG_PATH if not self.__argument.config else self.__argument.config
        input_directory = DEFAULT_INPUT_PATH if not self.__argument.input else self.__argument.input
        output_directory = DEFAULT_OUTPUT_PATH if not self.__argument.output else self.__argument.output
        config.input_directory = abspath(input_directory)
        config.output_directory = abspath(output_directory)
        print("Reading translation config file from %s..." % config_file_path)
        with open(config_file_path) as f:
            data = yaml.load(f, Loader=SafeLoader)
        self.__validate_config_yaml(data)

        gcp_settings_input = data["gcp_settings"]
        config.project_number = gcp_settings_input["project_number"]
        config.gcs_bucket = gcp_settings_input["gcs_bucket"]

        translation_config_input = data[self.__TRANSLATION_CONFIG]
        config.location = translation_config_input["location"]
        config.translation_type = translation_config_input[self.__TRANSLATION_TYPE]

        config.clean_up_tmp_files = True if self.__CLEAN_UP not in translation_config_input \
            else translation_config_input[self.__CLEAN_UP]

        if not os.path.exists(config.output_directory):
            os.makedirs(config.output_directory)

        print("Finished parsing translation config.")
        print("The config is:")
        print('\n'.join("     %s: %s" % item for item in vars(config).items()))
        return config

    def __validate_config_yaml(self, yaml_data):
        """Validate the data in the config yaml file.
        """
        assert self.__TRANSLATION_CONFIG in yaml_data, "Missing translation_config field in config.yaml."
        assert self.__TRANSLATION_TYPE in yaml_data[
            self.__TRANSLATION_CONFIG], "Missing translation_type field in config.yaml."
        translation_type = yaml_data[self.__TRANSLATION_CONFIG][self.__TRANSLATION_TYPE]
        assert translation_type in self.__SUPPORTED_TYPES, "The type \"%s\" is not supported." \
                                                           % translation_type


def validate_args(args: Namespace):
    """Validates the arguments passed to the tool
    """
    if not os.getcwd().endswith(REPO_ROOT_DIR):
        if not args.config:
            print("Warning: you are not running the binary from the repo's root dir \"%s\" and you did not "
                  "pass a config file path through \"--config\". Trying to use the default config path \"%s\"."
                  % (REPO_ROOT_DIR, DEFAULT_CONFIG_PATH))
        if not args.input:
            print("Warning: you are not running the binary from the repo's root dir \"%s\" and you did not "
                  "pass an input path through \"--input\". Trying to use the default input path \"%s\"." %
                  (REPO_ROOT_DIR, DEFAULT_INPUT_PATH))
        if not args.output:
            print("Warning: you are not running the binary from the repo's root dir \"%s\" and you did not "
                  "pass an output path through \"--output\". Trying to use the default output path \"%s\"." %
                  (REPO_ROOT_DIR, DEFAULT_OUTPUT_PATH))

    if args.input:
        assert exists(args.input), "Can't find an input directory at \"%s\"." % args.input
    else:
        assert exists(DEFAULT_INPUT_PATH), "Can't find a directory at the default input path \"%s\"" \
                                           % DEFAULT_INPUT_PATH
    if args.config:
        assert exists(args.config), "Can't find a config file at \"%s\"." % args.config
    else:
        assert exists(DEFAULT_CONFIG_PATH), "Can't find a file at the default config path \"%s\"" % DEFAULT_CONFIG_PATH
    if args.macros:
        assert exists(args.macros), "Can't find a file at \"%s\"." % args.macros
