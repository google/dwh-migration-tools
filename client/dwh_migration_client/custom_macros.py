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


"""Custom macro handling code goes here"""

import re
from argparse import Namespace

from dwh_migration_client.macro_expander import (
    MacroExpanderRouter,
    PatternMacroExpander,
    SimpleMacroExpander,
)
from dwh_migration_client.macro_processor import (
    MacroProcessor,
    parse_macros_config_file,
)


def custom_macros(args: Namespace) -> MacroProcessor:
    """Custom macro handling code should go here."""
    return simple_macros(args)


def simple_macros(args: Namespace) -> MacroProcessor:
    """This default implementation just creates a simple macro expander for each
    file glob found in the macros.yaml file. See the documentation for
    PatternMacroExpander and the example below if more complicated behavior is
    required."""
    mappings = parse_macros_config_file(args.macros)

    expanders = {}
    for glob, mapping in mappings.items():
        expanders[glob] = SimpleMacroExpander(mapping)
    return MacroProcessor(MacroExpanderRouter(expanders))


def custom_pattern_macros_example(args: Namespace) -> MacroProcessor:
    """An example of a more complicated macro processor. This is not
    used by default."""

    mappings = parse_macros_config_file(args.macros)

    # matches something like ${NAME}
    pattern = "\\$\\{(\\w+)\\}"

    # ${MACRO_NAME} -> MACRO_NAME_MACRO
    def expand(file_name: str, macro_name: str) -> str:
        return "MACRO_" + macro_name + "_MACRO"

    # MACRO_NAME_MACRO -> {NAME}
    def un_expand(file_name: str, replacement: str, macro_name: str) -> str:
        match = re.match("PARAM_(.+)_PARAM", replacement)
        if match:
            unexpanded = match.group(1)
        else:
            unexpanded = macro_name
        return "{" + unexpanded + "}"

    expanders = {}
    for glob in mappings.keys():
        expanders[glob] = PatternMacroExpander(
            pattern=pattern, generator=expand, un_generator=un_expand
        )
    return MacroProcessor(MacroExpanderRouter(expanders))
