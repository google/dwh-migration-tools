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
"""CLI validations for BigQuery Batch SQL Translator"""

import argparse
import pathlib
import shutil


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
