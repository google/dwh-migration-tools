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
"""CLI for BigQuery Batch SQL Translator"""

import argparse
import logging
import sys
from functools import partial
from typing import List

from marshmallow import ValidationError

from dwh_migration_client import batch_sql_translator
from dwh_migration_client.config import parse as parse_config
from dwh_migration_client.gcloud_auth_helper import validate_gcloud_auth_settings
from dwh_migration_client.macro_processor import MacroProcessor
from dwh_migration_client.object_name_mapping import parse as parse_object_name_mapping
from dwh_migration_client.validation import (
    validated_directory,
    validated_file,
    validated_nonexistent_path,
)


def start_translation(args: argparse.Namespace) -> None:
    """Starts a batch sql translation job."""
    try:
        config = parse_config(args.config)
    except ValidationError:
        sys.exit(1)

    if args.object_name_mapping:
        try:
            object_name_mapping_list = parse_object_name_mapping(
                args.object_name_mapping
            )
        except ValidationError:
            sys.exit(1)
    else:
        object_name_mapping_list = None

    if args.macros:
        try:
            preprocessor = MacroProcessor(args)
        except ValidationError:
            sys.exit(1)
    else:
        preprocessor = None

    logging.info("Verify cloud login and credential settings...")
    validate_gcloud_auth_settings(config.gcp_settings.project_number)

    translator = batch_sql_translator.BatchSqlTranslator(
        config, args.input, args.output, preprocessor, object_name_mapping_list
    )
    translator.start_translation()


def parse_args(args: List[str]) -> argparse.Namespace:
    """Argument parser for the BigQuery Batch SQL Translator CLI."""
    parser = argparse.ArgumentParser(
        description="Config the Batch Sql translation tool."
    )
    parser.add_argument(
        "--verbose", help="Increase output verbosity", action="store_true"
    )
    parser.add_argument(
        "--config",
        type=validated_file,
        default="client/config.yaml",
        help="Path to the config.yaml file. (default: client/config.yaml)",
    )
    parser.add_argument(
        "--input",
        type=validated_directory,
        default="client/input",
        help="Path to the input_directory. (default: client/input)",
    )
    parser.add_argument(
        "--output",
        type=partial(validated_nonexistent_path, force=True),
        default="client/output",
        help="Path to the output_directory. (default: client/output)",
    )
    parser.add_argument(
        "-m",
        "--macros",
        type=validated_file,
        help="Path to the macro map yaml file. If specified, the program will "
        "pre-process all the input query files by replacing the macros with "
        "corresponding string values according to the macro map definition. After "
        "translation, the program will revert the substitutions for all the output "
        "query files in a post-processing step.  The replacement does not apply for "
        "files with extension of .zip, .csv, .json.",
    )
    parser.add_argument(
        "-o",
        "--object_name_mapping",
        type=validated_file,
        help="Path to the object name mapping json file. Name mapping lets you "
        "identify the names of SQL objects in your source files, and specify target "
        "names for those objects in BigQuery. More info please see "
        "https://cloud.google.com/bigquery/docs/output-name-mapping.",
    )

    parsed_args = parser.parse_args(args)

    logging.basicConfig(
        level=logging.DEBUG if parsed_args.verbose else logging.INFO,
        format="%(asctime)s: %(levelname)s: %(message)s",
    )

    return parsed_args


def main() -> None:
    """CLI for BigQuery Batch SQL Translator"""
    args = parse_args(sys.argv[1:])
    return start_translation(args)


if __name__ == "__main__":
    main()
