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


import argparse
import batch_sql_translator

from config_parser import ConfigParser
from gcloud_auth_helper import validate_gcloud_auth_settings
from sql_processor import MacrosMapParser

parser = argparse.ArgumentParser(description='Config the Batch Sql translation tool.')
parser.add_argument('-m', '--macro_map', type=str,
                    help='Path to the macro map yaml file. If specified, the program will pre-process '
                         'all the input query files by replacing the macros with corresponding '
                         'string values according to the macro map definition. After translation, '
                         'the program will revert the substitutions for all the output query files in a '
                         'post-processing step.  The replacement does not apply for files with extension of '
                         '.zip, .csv, .json.')
args = parser.parse_args()


def start_translation():
    """Starts a batch sql translation job.
    """
    print("Reading translation config file from config.yaml")
    config = ConfigParser().parse_config()
    print("Verify cloud login and credential settings...")
    validate_gcloud_auth_settings(config.project_number)
    if args.macro_map:
        print('Reading macros replacement map from %s' % args.macro_map)
        config.macro_maps = MacrosMapParser(args.macro_map).parse()
    translator = batch_sql_translator.BatchSqlTranslator(config)
    translator.start_translation()


if __name__ == '__main__':
    start_translation()
