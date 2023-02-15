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
"""Processors for heredoc SQL embedded in KSH code to translate."""

import logging
import re
from enum import Enum
from typing import List, Optional

logger = logging.getLogger(__name__)


class ShellFragmentType(Enum):
    SHELL = "SHELL"
    HEREDOC = "HEREDOC"  # aka bteq
    EMPTY = "EMPTY"  # a special one


class ShellFragment:
    """
    A fragment of a KSH file.
    """

    def __init__(
        self, text: str, line: int, fragment_type: ShellFragmentType, quoted: bool
    ) -> None:
        super().__init__()
        self.text = text
        self.line = line
        self.fragment_type = fragment_type
        self.quoted = quoted

    def __str__(self) -> str:
        return (
            "ShellFragment{"
            + ",".join(
                [
                    f"line={self.line}",
                    f"fragment_type={self.fragment_type}",
                    f"quoted={self.quoted}",
                    f"text={self.text}",
                ]
            )
            + "}"
        )

    def get_fragment_type(self) -> ShellFragmentType:
        return self.fragment_type

    def get_text(self) -> str:
        return self.text


class ExtractorState(Enum):
    SHELL = "SHELL"
    BTEQ = "BTEQ"


class KshExtractor:
    """
    An extractor capable of splitting KSH input into fragments.
    """

    PATTERN_DEFAULT_PREFIX = r"(?:\s*|/)?"
    PATTERN_DEFAULT_SUFFIX = r"\s*[^<]*<<[-#]?\s*([^\s><]+)"

    def __init__(
        self,
        command: str,
        command_replace_from: Optional[str] = None,
        command_replace_to: Optional[str] = None,
    ) -> None:
        """
        :param command: the heredoc command to search for
        :param command_replace_from: the prefix of the heredoc command
        :param command_replace_to: the suffix of the heredoc command
        """
        super().__init__()
        self.pattern = self._check(
            re.compile(KshExtractor.to_pattern_for_command(command))
        )
        self.command_replace_from = command_replace_from
        self.command_replace_to = command_replace_to

    @staticmethod
    def to_pattern_for_command(command: str) -> str:
        return (
            KshExtractor.PATTERN_DEFAULT_PREFIX
            + command
            + KshExtractor.PATTERN_DEFAULT_SUFFIX
        )

    @staticmethod
    def _check(pattern: re.Pattern) -> re.Pattern:  # type: ignore[type-arg]
        """
        Ensures that the provided pattern contains only a single capture group.
        :param pattern: the pattern to check
        :return: the pattern if it is valid, raises an exception otherwise
        """
        if pattern.groups != 1:
            raise ValueError(
                "Pattern must have a single capturing group to "
                + "capture the heredoc end-marker, but does not: "
                + f"'{pattern.pattern}'."
            )
        return pattern

    @staticmethod
    def filter_heredoc_sql_texts(fragments: List[ShellFragment]) -> List[str]:
        """
        Filters out heredoc SQL texts from a collection of fragments.
        :param fragments: a list of fragments
        :return: the SQL texts of any heredoc fragments in the input list
        """
        return list(
            map(
                lambda f: f.get_text(),
                filter(
                    lambda f: f.get_fragment_type() == ShellFragmentType.HEREDOC,
                    fragments,
                ),
            )
        )

    @staticmethod
    def move_fragment(
        out: List[ShellFragment],
        buf: List[str],
        start_line_number: int,
        state: ExtractorState,
        eod: Optional[str],
    ) -> None:
        """
        Adds a heredoc fragment to the output list.
        """
        if len(buf) == 0:
            return
        quoted = eod is not None and (eod[0] == "'" or eod[0] == '"')
        out.append(
            ShellFragment(
                "".join(buf),
                start_line_number,
                ShellFragmentType.HEREDOC
                if state == ExtractorState.BTEQ
                else ShellFragmentType.SHELL,
                quoted,
            )
        )
        buf.clear()

    def replace_command(self, line: str) -> str:
        if self.command_replace_from is None or self.command_replace_to is None:
            return line
        return line.replace(self.command_replace_from, self.command_replace_to, 1)

    def read_fragments(self, input_text: str) -> List[ShellFragment]:
        """
        Splits an input string presumed to be KSH into zero or more fragments.
        :param input_text: the KSH text to read fragments from
        :return: the list of fragments found
        """
        out: List[ShellFragment] = []
        buf: List[str] = []
        eod: Optional[str] = None
        unquoted_eod: Optional[
            str
        ] = None  # with leading and trailing ' "" removed, if any
        state: ExtractorState = ExtractorState.SHELL
        line_number = 0
        start_line_number = 0
        for line in input_text.splitlines():
            logger.debug(
                "state=%s, line_number=%d, line=%s, buf=%s",
                state,
                line_number,
                line,
                buf,
            )
            if state == ExtractorState.SHELL:
                # strip off the text after #. This should be mostly GOOD.
                non_comment = line.split("#")[0]
                match = self.pattern.search(non_comment)
                logger.debug("non_comment=%s, match=%s", non_comment, match)
                if match:
                    eod = match.group(1)
                    unquoted_eod = (
                        eod[1 : len(eod) - 1] if eod[0] == "'" or eod[0] == '"' else eod
                    )
                    # include the begin HEREDOC ; there may be comments
                    buf.extend([self.replace_command(line), "\n"])
                    self.move_fragment(
                        out, buf, start_line_number, ExtractorState.SHELL, eod
                    )
                    # prepare for Next ...
                    state = ExtractorState.BTEQ
                    start_line_number = line_number + 1
                else:
                    buf.extend([line, "\n"])
            else:
                # TODO: This needs to strip leading tabs in the - case.
                # TODO: equals eod is probably wrong
                logger.debug("unquoted_eod=%s, eod=%s", unquoted_eod, eod)
                if line in (unquoted_eod, eod):
                    self.move_fragment(
                        out, buf, start_line_number, ExtractorState.BTEQ, eod
                    )
                    eod = None
                    state = ExtractorState.SHELL
                    start_line_number = line_number
                    # include the end of HEREDOC
                    buf.extend([line, "\n"])
                else:
                    buf.extend([line, "\n"])
            line_number += 1
        if state == ExtractorState.BTEQ:
            logger.warning("Here-document terminates at EOF in input_text.")
        self.move_fragment(out, buf, start_line_number, state, eod)
        return out
