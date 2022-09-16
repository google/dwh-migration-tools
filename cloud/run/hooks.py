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
"""User-defined code that is hooked into the translation workflow."""
from run.paths import Path


def preprocess(path: Path, text: str) -> str:  # pylint: disable=unused-argument
    """Preprocesses input via user-defined code before submitting it to BQMS.

    Args:
        path: A run.paths.Path representing the relative path of the input to be
            preprocessed.
        text: A string representing the contents of the input to be
            preprocessed.

    Returns:
        A string representing the preprocesssed input.
    """
    return text


def postprocess(path: Path, text: str) -> str:  # pylint: disable=unused-argument
    """Postprocesses translated BQMS output via user-defined code.

    Args:
        path: A run.paths.Path representing the relative path of the translated
            output to be postprocessed.
        text: A string representing the contents of the translated output to be
            postprocessed.

    Returns:
        A string representing the postprocessed translated output.
    """
    return text
