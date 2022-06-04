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
from functools import partial
import shutil
import pathlib
import sys
import batch_sql_translator

from config_parser import ConfigParser
from gcloud_auth_helper import validate_gcloud_auth_settings
from macro_processor import MacroProcessor
from object_mapping_parser import ObjectMappingParser


def start_translation(args):
    """Starts a batch sql translation job.
    """
    config = ConfigParser(args.config).parse_config()

    print("\nVerify cloud login and credential settings...")
    validate_gcloud_auth_settings(config.project_number)

    if args.macros:
        preprocessor = MacroProcessor(args)
    else:
        preprocessor = None

    if args.object_name_mapping:
        object_name_mapping_list = \
            ObjectMappingParser(args.object_name_mapping).get_name_mapping_list()
    else:
        object_name_mapping_list = None

    translator = batch_sql_translator.BatchSqlTranslator(config, args.input, args.output, preprocessor, object_name_mapping_list)
    translator.start_translation()


def validated_file(unvalidated_path: str) -> str:
    """Validates a path is a regular file that exists.
    Args:
        unvalidated_path: A string representing the path to validate.
    Returns:
        A string representing a validated POSIX path.
    Raises:
        argparse.ArgumentTypeError: unvalidated_path is not a regular file that exists.
    """
    path = pathlib.Path(unvalidated_path)
    if path.is_file():
        return path.as_posix()
    raise argparse.ArgumentTypeError(
        "%s is not a regular file that exists." % path.as_posix()
    )


def validated_directory(unvalidated_path: str) -> str:
    """Validates a path is a directory that exists.
    Args:
        unvalidated_path: A string representing the path to validate.
    Returns:
        A string representing a validated POSIX path.
    Raises:
        argparse.ArgumentTypeError: unvalidated_path is not a directory that exists.
    """
    path = pathlib.Path(unvalidated_path)
    if path.is_dir():
        return path.as_posix()
    raise argparse.ArgumentTypeError(
        "%s is not a directory that exists." % path.as_posix()
    )


def validated_nonexistent_path(unvalidated_path: str, force: bool = False) -> str:
    """Validates a path does not exist.
    Args:
        unvalidated_path: A string representing the path to validate.
        force: A boolean representing whether to remove unvalidated_path if it exists.
    Returns:
        A string representing a validated POSIX path.
    Raises:
        argparse.ArgumentTypeError: unvalidated_path already exists.
    """
    path = pathlib.Path(unvalidated_path)

    if not path.exists():
        return path.as_posix()

    if force:
        if path.is_dir():
            shutil.rmtree(path)
        if path.is_file():
            path.unlink()
        return path.as_posix()

    raise argparse.ArgumentTypeError("%s already exists." % path.as_posix())


def main():
    parser = argparse.ArgumentParser(description='Config the Batch Sql translation tool.')
    parser.add_argument('--config', type=validated_file, default="client/config.yaml", help='Path to the config.yaml file.')
    parser.add_argument('--input', type=validated_directory, default="client/input", help='Path to the input_directory.')
    parser.add_argument('--output', type=partial(validated_nonexistent_path, force=True), default="client/output", help='Path to the input_directory.')
    parser.add_argument('-m', '--macros', type=validated_file,
                        help='Path to the macro map yaml file. If specified, the program will pre-process '
                             'all the input query files by replacing the macros with corresponding '
                             'string values according to the macro map definition. After translation, '
                             'the program will revert the substitutions for all the output query files in a '
                             'post-processing step.  The replacement does not apply for files with extension of '
                             '.zip, .csv, .json.')
    parser.add_argument('-o', '--object_name_mapping', type=validated_file,
                        help='Path to the object name mapping json file. Name mapping lets you identify the names of SQL '
                             'objects in your source files, and specify target names for those objects in BigQuery. More '
                             'info please see https://cloud.google.com/bigquery/docs/output-name-mapping.')

    # Print usage message if no args are supplied.
    if len(sys.argv) <= 1:
        sys.argv.append("--help")

    args = parser.parse_args()
    return start_translation(args)

if __name__ == '__main__':
    main()
