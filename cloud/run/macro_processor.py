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
"""Text processor for macro/templating langs embedded in code to translate."""

import fnmatch
import re
from collections.abc import Mapping
from pprint import pformat
from typing import Pattern, Tuple

from marshmallow import Schema, fields, post_load

from run.paths import Path


class _MacroProcessorSchema(Schema):
    """Schema and validator for MacroProcessor."""

    macros = fields.Dict(
        keys=fields.String(),
        values=fields.Dict(keys=fields.String(), values=fields.String(), required=True),
        required=True,
    )

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return MacroProcessor(data["macros"])


class MacroProcessor:
    """Text processor for macro/templating languages in code to be translated.

    Example:
        .. code-block::

            macro_processor = MacroProcessor.from_mapping({
                "macros" {
                    "*.sql": {
                        "${MACRO_1}": "my_col"
                        "%MACRO_2%": "my_table"
                    },
                    "date-truncation.sql": {
                        "demo_date_truncation_table": "foo"
                        "demo_date": "2022-09-01"
                    },
                }
            })

            relative_path = pathlib.Path("foo.sql")

            input_text = "select ${MACRO_1} from my_db.%MACRO_2%;"
            expanded_text = macro_processor.expand(relative_path, input_text)
            print(expanded_text)  # select my_col from my_db.my_table;

            translated_text = "SELECT my_table.my_col FROM my_db.my_table;"
            unexpanded_text = macro_processor.unexpand(relative_path, translated_text)
            print(unexpanded_text)  # SELECT %MACRO_2%.${MACRO_1} FROM my_db.%MACRO_2%;

    Args:
        macro_mapping: A Mapping of globs to Mapping of macro name to macro
            value. See Example.
    """

    def __init__(self, macro_mapping: Mapping[str, Mapping[str, str]]) -> None:
        self._macro_mapping = macro_mapping
        self._reversed_maps = self._get_reversed_maps()
        self._case_insensitive_re_pattern = r"(?i)"

    @staticmethod
    def from_mapping(
        mapping: Mapping[str, Mapping[str, Mapping[str, str]]]
    ) -> "MacroProcessor":
        """Factory method for creating a MacroProcessor instance from a Mapping.

        Args:
            mapping: A Mapping of globs to Mapping of macro name to macro
                value. See class-level Example.

        Returns:
            A MacroProcessor instance.
        """
        macro_processor: MacroProcessor = _MacroProcessorSchema().load(mapping)
        return macro_processor

    def expand(self, path: Path, text: str) -> str:
        """Expands macro/template expressions embedded in input to be translated.

        Args:
            path: A run.paths.Path representing the relative path of the input to
                be translated.
            text: A string representing the contents of the input to be
                translated.

        Returns:
            A string representing the expanded input.
        """
        reg_pattern_map, patterns = self._get_all_regex_pattern_mapping(str(path))
        if not reg_pattern_map:
            return text
        return patterns.sub(lambda m: reg_pattern_map[re.escape(m.group())], text)

    def unexpand(self, path: Path, text: str) -> str:
        """Unexpands macro/template expressions embedded in translated output.

        Args:
            path: A run.paths.Path representing the relative path of the
                translated output.
            text: A string representing the contents of the translated output.

        Returns:
            A string representing the unexpanded translated output.
        """
        reg_pattern_map, patterns = self._get_all_regex_pattern_mapping(str(path), True)
        if not reg_pattern_map:
            return text
        return re.sub(
            patterns, lambda m: reg_pattern_map[re.escape(m.group().lower())], text
        )

    def _get_reversed_maps(self) -> Mapping[str, Mapping[str, str]]:
        reversed_maps = {}
        for file_key, macro_map in self._macro_mapping.items():
            reversed_maps[file_key] = dict((v, k) for k, v in macro_map.items())
        return reversed_maps

    def _get_all_regex_pattern_mapping(
        self, file_path: str, use_reversed_map: bool = False
    ) -> Tuple[Mapping[str, str], Pattern[str]]:
        macro_subst_maps = (
            self._reversed_maps if use_reversed_map else self._macro_mapping
        )
        reg_pattern_map = {}
        for file_map_key, token_map in macro_subst_maps.items():
            if fnmatch.fnmatch(file_path, file_map_key):
                for key, value in token_map.items():
                    # Converts the keys to lower case during macro unexpansion
                    # to support case-insensitive pattern matching.
                    reg_pattern_map[
                        re.escape(key.lower()) if use_reversed_map else re.escape(key)
                    ] = value
        all_regex_pattern_str = "|".join(reg_pattern_map.keys())
        if use_reversed_map:
            all_regex_pattern_str = (
                self._case_insensitive_re_pattern + all_regex_pattern_str
            )
        all_patterns = re.compile(all_regex_pattern_str)
        return reg_pattern_map, all_patterns

    def __repr__(self) -> str:
        return pformat(self._macro_mapping)
