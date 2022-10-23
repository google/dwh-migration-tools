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
import re
from collections.abc import Mapping

from bqms_run.macros import (
    MacroExpanderRouter,
    PatternMacroExpander,
    SimpleMacroExpander,
)
from bqms_run.paths import Path


def preprocess(path: Path, text: str) -> str:  # pylint: disable=unused-argument
    """Preprocesses input via user-defined code before submitting it to BQMS.

    Args:
        path: Abqms_run.paths.Path representing the relative path of the input to be
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
        path: Abqms_run.paths.Path representing the relative path of the translated
            output to be postprocessed.
        text: A string representing the contents of the translated output to be
            postprocessed.

    Returns:
        A string representing the postprocessed translated output.
    """
    return text


def custom_macros(mappings: Mapping[str, Mapping[str, str]]) -> MacroExpanderRouter:
    """Custom macro handling code should go here."""
    return simple_macros(mappings)


def simple_macros(mappings: Mapping[str, Mapping[str, str]]) -> MacroExpanderRouter:
    """This default implementation just creates a simple macro expander for each
    file glob found in the macros.yaml file. See the documentation for
    PatternMacroExpander and the example below if more complicated behavior is
    required."""
    expanders = {}
    for glob, mapping in mappings.items():
        expanders[glob] = SimpleMacroExpander(mapping)
    return MacroExpanderRouter(expanders)


def custom_pattern_macros_example(
    mappings: Mapping[str, Mapping[str, str]]
) -> MacroExpanderRouter:
    """An example of a more complicated macro processor. This is not
    used by default."""
    # matches something like ${NAME}
    pattern = "\\$\\{(\\w+)\\}"

    # ${MACRO_NAME} -> MACRO_NAME_MACRO
    def expand(path: Path, macro_name: str) -> str:  # pylint: disable=unused-argument
        return "MACRO_" + macro_name + "_MACRO"

    # MACRO_NAME_MACRO -> {NAME}
    def un_expand(
        path: Path, replacement: str, macro_name: str  # pylint: disable=unused-argument
    ) -> str:
        match = re.match("PARAM_(.+)_PARAM", replacement)
        if match:
            unexpanded = match.group(1)
        else:
            unexpanded = macro_name
        return "{" + unexpanded + "}"

    expanders = {}
    for glob, mapping in mappings.items():
        expanders[glob] = PatternMacroExpander(
            pattern, mapping, generator=expand, un_generator=un_expand
        )
    return MacroExpanderRouter(expanders)
