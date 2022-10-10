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

"""Used to run just the macro processing code on input files, without performing
any of the other steps of batch processing. May be useful for debugging custom
macro code."""

import argparse
import logging
import sys
from os.path import join
from typing import List

from dwh_migration_client.custom_macros import custom_macros
from dwh_migration_client.validation import validated_directory, validated_file


def main() -> None:
    tmp_dir_name = ".tmp_processed"
    args = _parse_args(sys.argv[1:])
    processor = custom_macros(args)
    tmp_dir = join(args.input, tmp_dir_name)
    print(f"Placing processed sql files in {tmp_dir}")
    processor.preprocess(args.input, tmp_dir)


def _parse_args(args: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the macro processor on the given input files."
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
        "--verbose", help="Increase output verbosity", action="store_true"
    )
    parser.add_argument(
        "--input",
        type=validated_directory,
        default="client/input",
        help="Path to the input_directory. (default: client/input)",
    )

    parsed_args = parser.parse_args(args)

    logging.basicConfig(
        level=logging.DEBUG if parsed_args.verbose else logging.INFO,
        format="%(asctime)s: %(levelname)s: %(message)s",
    )

    return parsed_args


if __name__ == "__main__":
    main()
