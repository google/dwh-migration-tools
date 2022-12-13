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
"""Processors for macro/templating langs embedded in code to translate."""

import logging
import re
from fnmatch import fnmatch
from typing import Callable, Dict, List, Mapping, Match, Optional, Set, Tuple

from marshmallow import Schema, fields, post_load

from bqms_run.paths import Path

logger = logging.getLogger(__name__)


class MacroMapping(Schema):
    """Schema and validator for macro mapping."""

    macros = fields.Dict(
        keys=fields.String(),
        values=fields.Dict(keys=fields.String(), values=fields.String(), required=True),
        required=True,
    )

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return data["macros"]

    @classmethod
    def from_mapping(
        cls, mapping: Mapping[str, Mapping[str, Mapping[str, str]]]
    ) -> Mapping[str, Mapping[str, str]]:
        """Returns a validated macro mapping."""
        macro_mapping: Mapping[str, Mapping[str, str]] = cls().load(mapping)
        return macro_mapping


class RecordingLogger:  # pylint: disable=too-few-public-methods
    """Logger that keeps track of the messages that it has logged."""

    def __init__(self) -> None:
        self.messages: List[str] = []

    def warn_log(self, format_string: str, *args: str) -> None:
        message = format_string.format(*args)
        self.messages.append(message)
        logger.warning(message)


class MacroExpander(RecordingLogger):
    """Base class for macro expanders. Do not use this directly."""

    def __init__(self, mapping: Mapping[str, str]) -> None:
        super().__init__()
        # macro name -> replacement
        self.mapping = mapping
        # path -> [replacement -> (count, original(s))]
        self.reverse: Dict[Path, Dict[str, Tuple[int, Set[str]]]] = {}

    def _update_reverse_map(
        self, path: Path, replacement: str, macro_name: str
    ) -> None:
        if not path in self.reverse:
            self.reverse[path] = {}
        reverse_for_path = self.reverse[path]
        if replacement in reverse_for_path:
            reverse_for_path[replacement] = (
                reverse_for_path[replacement][0] + 1,
                reverse_for_path[replacement][1].union({macro_name}),
            )
        else:
            reverse_for_path[replacement] = (1, {macro_name})

    def _sanity_check(self, path: Path) -> None:
        for replacement, originals in self.reverse[path].items():
            if len(originals[1]) > 1:
                self.warn_log(
                    "The value '{0}' was expanded from "
                    + "the following macros: {1}. Un-expansion will not "
                    + "be accurate.",
                    replacement,
                    str(originals[1]),
                )

    def expand(self, path: Path, text: str) -> str:
        pass

    def un_expand(self, path: Path, text: str) -> str:
        pass


class SimpleMacroExpander(MacroExpander):
    """
    Simple macro expander. Does not use regular expressions or generator
    functions.
    """

    def expand(self, path: Path, text: str) -> str:
        for macro_name, substitution_text in self.mapping.items():
            if text.find(macro_name) != -1:
                text = text.replace(macro_name, substitution_text)
                self._update_reverse_map(path, substitution_text, macro_name)
        return text

    def un_expand(self, path: Path, text: str) -> str:
        if path not in self.reverse:
            return text
        self._sanity_check(path)
        for substitution_text, original in self.reverse[path].items():
            original_text = original[1].pop()
            text = text.replace(substitution_text, original_text)
        return text


class PatternMacroExpander(MacroExpander):
    """Handles expanding and un-expanding macros in a user-customizable way."""

    def __init__(
        self,
        pattern: str,
        mapping: Mapping[str, str],
        generator: Optional[Callable[[Path, str], str]] = None,
        un_generator: Optional[Callable[[Path, str, str], str]] = None,
    ) -> None:
        """
        Args:
            pattern: A regular expression that should match the macros in the
                input paths. The expression should have a group that matches the
                name of the macro.
            mapping: An optional mapping of macro names to their expanded value
            generator: An optional function that will be used to expand macros
                that were not matched by the 'mapping'. Takes the path and
                the macro name (from the 'pattern' group) and returns a string.
            un_generator: An optional function that will be used to un-expand a
                macro. Takes a path, the fully expanded macro, and its
                original name. Returns the text to substitute back into the
                result as a string.
        """
        super().__init__(mapping)
        self.pattern = re.compile(pattern, re.I)
        self.generator = generator
        self.un_generator = un_generator
        # path -> unmapped macros
        self.unmapped: Dict[Path, Set[str]] = {}

    def _substitution(self, path: Path, match: Match[str]) -> str:
        macro_name = match.group(1)
        full_match = match.group(0)
        if self.mapping and macro_name in self.mapping:
            generated = self.mapping[macro_name]
        elif self.generator:
            if path in self.unmapped:
                self.unmapped[path].union(macro_name)
            else:
                self.unmapped[path] = {macro_name}
            generated = self.generator(path, macro_name)
        else:
            self.warn_log(
                "Could not expand '{0}' as it is not "
                + "present in the mapping and no generator function was "
                + "provided",
                full_match,
            )
            generated = full_match
        self._update_reverse_map(path, generated, full_match)
        return generated

    def expand(self, path: Path, text: str) -> str:
        """
        Expands all macros in the given input text
        Args:
            path: The path that the 'text' comes from
            text: The text containing macro uses
        Returns: 'text' with its macro uses expanded
        """
        return self.pattern.sub(lambda match: self._substitution(path, match), text)

    def un_expand(self, path: Path, text: str) -> str:
        """
        Reverses the macro expansion done by 'expand'.
        Args:
            path: The path that the 'text' comes from.
            text: The text resulting from invoking this expander on some input.
        Returns: 'text' with the macro expansions undone.
        """
        if path not in self.reverse:
            return text
        self._sanity_check(path)
        for replacement, original in self.reverse[path].items():
            original_text = original[1].pop()
            if self.un_generator:
                result = re.subn(
                    re.escape(replacement),
                    self.un_generator(path, replacement, original_text),
                    text,
                    flags=re.I,
                )
            else:
                result = re.subn(re.escape(replacement), original_text, text)
            text = result[0]
        return text


class MacroExpanderRouter(RecordingLogger):
    """
    Contains multiple 'MacroExpander' instances and decided which to use
    based on paths and glob patterns.
    """

    def __init__(self, all_macros: Mapping[str, MacroExpander]) -> None:
        """
        Args:
            all_macros: A mapping of glob patterns to macro expanders.
        """
        super().__init__()
        self.all_macros = all_macros

    def _choose_expanders(self, path: Path) -> List[MacroExpander]:
        chosen_expanders = []
        for pattern, expander in self.all_macros.items():
            if fnmatch(str(path), pattern):
                chosen_expanders.append(expander)
        return chosen_expanders

    def expand(self, path: Path, text: str) -> str:
        """
        Calls 'expand' on the correct macro expander based on the path
        """
        expanders = self._choose_expanders(path)
        for expander in expanders:
            text = expander.expand(path, text)
        return text

    def un_expand(self, path: Path, text: str) -> str:
        """
        Calls 'un_expand' on the correct macro expander based on the path
        """
        expanders = self._choose_expanders(path)
        for expander in expanders:
            text = expander.un_expand(path, text)
        return text

    def all_messages(self) -> List[str]:
        """
        Return all warning/error messages that the macro expanders generated.
        """
        result = self.messages
        for _, expander in self.all_macros.items():
            result = result + expander.messages
        return result
