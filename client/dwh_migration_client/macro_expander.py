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

""" Macro handling classes """

import logging
import re
from fnmatch import fnmatch
from typing import Callable, Dict, List, Optional, Set, Tuple


class RecordingLogger:
    """Logger that keeps track of the messages that it has logged."""

    def __init__(self) -> None:
        self.messages = []

    def warn_log(self, format_string, *args) -> None:
        message = format_string.format(*args)
        self.messages.append(message)
        logging.warning(message)


class MacroExpander(RecordingLogger):
    """Base class for macro expanders. Do not use this directly."""

    def __init__(self, mapping: Optional[Dict[str, str]] = None) -> None:
        super().__init__()
        # macro name -> replacement
        self.mapping = mapping
        # replacement -> macro_name. May contain fewer entries than 'mapping'
        self.reverse = {}

    def _sanity_check(self, file_name: str) -> None:
        for replacement, originals in self.reverse[file_name].items():
            if len(originals[1]) > 1:
                self.warn_log(
                    "The value '{0}' was expanded from "
                    + "the following macros: {1}. Un-expansion will not "
                    + "be accurate.",
                    replacement,
                    originals[1],
                )

    def expand(self, file_name: str, text: str) -> str:
        pass

    def un_expand(self, file_name: str, text: str) -> str:
        pass


class SimpleMacroExpander(MacroExpander):
    """
    Simple macro expander. Does not use regular expressions or generator
    functions.
    """

    def expand(self, file_name: str, text: str) -> str:
        for macro_name, substitution_text in self.mapping.items():
            text = text.replace(macro_name, substitution_text)
            if file_name in self.reverse:
                for_file = self.reverse[file_name]
                existing_macro_name = substitution_text in for_file
                if existing_macro_name and macro_name != existing_macro_name:
                    self.warn_log(
                        "The value '{0}' was expanded from "
                        + "the following macros: {1}, {2}. Un-expansion will not "
                        + "be accurate.",
                        substitution_text,
                        macro_name,
                        existing_macro_name,
                    )
                self.reverse[file_name][substitution_text] = macro_name
            else:
                self.reverse[file_name] = {substitution_text: macro_name}
        return text

    def un_expand(self, file_name: str, text: str) -> str:
        for substitution_text, macro_name in self.reverse[file_name].items():
            text = text.replace(substitution_text, macro_name)
        return text


class PatternMacroExpander(MacroExpander):
    """Handles expanding and un-expanding macros in a user-customizable way."""

    def __init__(
        self,
        pattern: str,
        mapping: Optional[Dict[str, str]] = None,
        generator: Optional[Callable[[str, str], str]] = None,
        un_generator: Optional[Callable[[str, str, str], str]] = None,
    ) -> None:
        """
        Args:
            pattern: A regular expression that should match the macros in the
                input files. The expressin should have a group that matches the
                name of the macro.
            mapping: An optional mapping of macro names to their expanded value
            generator: An optional function that will be used to expand macros
                that were not matched by the 'mapping'. Takes the file name and
                the macro name (from the 'pattern' group) and returns a string.
            un_generator: An optional function that will be used to un-expand a
                macro. Takes a file name, the fully expanded macro, and its
                original name. Returns a the text to substitute back into the
                result as a string.
        """
        super().__init__(mapping)
        self.pattern = re.compile(pattern, re.I)
        self.generator = generator
        self.un_generator = un_generator
        # file name -> unmapped macros
        self.unmapped = {}
        # file name -> [replacement -> original(s)]
        self.reverse = {}

    def _substitution(self, file_name: str, match) -> str:
        macro_name = match.group(1)
        full_match = match.group(0)
        generated = None
        if self.mapping and macro_name in self.mapping:
            generated = self.mapping[macro_name]
        elif self.generator:
            if file_name in self.unmapped:
                self.unmapped[file_name].union(macro_name)
            else:
                self.unmapped[file_name] = {macro_name}
            generated = self.generator(file_name, macro_name)
        else:
            self.warn_log(
                "Could not expand '{0}' as it is not "
                + "present in the mapping and no generator function was "
                + "provided",
                full_match,
            )
            generated = full_match
        if not file_name in self.reverse:
            self.reverse[file_name] = {}
        reverse_for_file = self.reverse[file_name]
        if generated in reverse_for_file:
            reverse_for_file[generated] = (
                reverse_for_file[generated][0] + 1,
                reverse_for_file[generated][1].union({full_match}),
            )
        else:
            reverse_for_file[generated] = (1, {full_match})
        return generated

    def expand(self, file_name: str, text: str) -> str:
        """
        Expands all macros in the given input text

        Args:
            file_name: The name of the file that the 'text' comes from
            text: The text containing macro uses
        Returns: 'text' with its macro uses expanded
        """
        return self.pattern.sub(
            lambda match: self._substitution(file_name, match), text
        )

    def un_expand(self, file_name: str, text: str) -> str:
        """
        Reverses the macro expansion done by 'expand'.

        Args:
            file_name: The name of the file that the 'text' comes from.
            text: The text resulting from invoking this expander on some input.
        Returns: 'text' with the macro expansions undone.
        """
        if not file_name in self.reverse:
            return text
        self._sanity_check(file_name)
        for replacement, original in self.reverse[file_name].items():
            result = None
            original_text = original[1].pop()
            if self.un_generator:
                result = re.subn(
                    re.escape(replacement),
                    self.un_generator(file_name, replacement, original_text),
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
    based on file names and file glob patterns.
    """

    all_macros: Dict[str, MacroExpander]

    def __init__(self, all_macros: Dict[str, MacroExpander]) -> None:
        """
        Args:
            all_macros: A mapping of file glob patterns to macro expanders.
        """
        super().__init__()
        self.all_macros = all_macros

    def _choose_expander(self, file_name: str) -> Optional[MacroExpander]:
        chosen_expander = None
        chosen_pattern = None
        matches = []
        for pattern, expander in self.all_macros.items():
            if fnmatch(file_name, pattern):
                matches.append(file_name)
                chosen_expander = expander
                chosen_pattern = pattern
        if len(matches) == 0:
            return None
        if len(matches) > 1:
            self.warn_log(
                "File name {0} matches multiple patterns. Arbitrarily choosing '{1}'.",
                file_name,
                chosen_pattern,
            )
        return chosen_expander

    def expand(self, file_name: str, text: str) -> str:
        """
        Calls 'expand' on the correct macro expander based on the file name
        """
        expander = self._choose_expander(file_name)
        if expander:
            return expander.expand(file_name, text)
        return text

    def un_expand(self, file_name: str, text: str) -> str:
        """
        Calls 'un_expand' on the correct macro expander based on the file name
        """
        expander = self._choose_expander(file_name)
        if expander:
            return expander.un_expand(file_name, text)
        return text

    def all_messages(self) -> List[str]:
        """
        Return all warning/error messages that the macro expanders generated.
        """
        result = self.messages
        for _, expander in self.all_macros.items():
            result = result + expander.messages
        return result
