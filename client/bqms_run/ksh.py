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
"""Processors for KSH embedded in code to translate."""

import logging
import re
from enum import Enum
from fnmatch import fnmatch
from typing import Callable, Dict, List, Mapping, Match, Optional, Set, Tuple

from marshmallow import Schema, fields, post_load

from bqms_run.paths import Path

logger = logging.getLogger(__name__)


class ShellFragmentType(Enum):
    SHELL = 'SHELL'
    HEREDOC = 'HEREDOC' # aka bteq
    EMPTY = 'EMPTY' # a special one


class ShellFragment:
    """
    TODO DESCRIBE
    """

    def __init__(
        self,
        text: str,
        line: int,
        ty: ShellFragmentType,
        quoted: bool
    ) -> None:
        """
        TODO: DESCRIBE
        :param text:
        :param line: zero-based
        :param ty:
        :param quoted:
        """
        super().__init__()
        self.text = text
        self.line = line
        self.ty = ty
        self.quoted = quoted

    def __str__(self) -> str:
        return 'ShellFragment{' + ','.join([
            f"line={self.line}",
            f"ty={self.ty}",
            f"quoted={self.quoted}",
            f"text={self.text}",
        ]) + '}'


class ExtractorState(Enum):
    SHELL = 'SHELL'
    BTEQ = 'BTEQ'


class KshExtractor:
    """
    TODO: DESCRIBE
    """
    PATTERN_DEFAULT_PREFIX = r'(?:\s*|/)?'
    PATTERN_DEFAULT_SUFFIX = r'\s*[^<]*<<[-#]?\s*([^\s><]+)'

    def __init__(
        self,
        command: str,
        command_replace_from: Optional[str] = None,
        command_replace_to: Optional[str] = None
    ) -> None:
        """
        TODO: DESCRIBE
        """
        super().__init__()
        self.pattern = self._check(KshExtractor.to_pattern_for_command(command))
        self.command_replace_from = command_replace_from
        self.command_replace_to = command_replace_to

    @staticmethod
    def to_pattern_for_command(command: str) -> re.Pattern:
        return re.compile(KshExtractor.PATTERN_DEFAULT_PREFIX + command + KshExtractor.PATTERN_DEFAULT_SUFFIX)

    @staticmethod
    def _check(pattern: re.Pattern) -> re.Pattern:
        """
        TODO DESCRIBE
        """
        logger.debug("Checking: pattern=%s, groups=%s", pattern.pattern, pattern.groups)
        if pattern.groups != 1:
            raise ValueError("Pattern must have a single capturing group to "
                             + "capture the heredoc end-marker, but does not: "
                             + f"'{pattern.pattern}'.")
        return pattern


    @staticmethod
    def move_fragment(
        out: List[ShellFragment],
        buf: List[str],
        start_line_number: int,
        state: ExtractorState,
        eod: Optional[str]) -> None:
        """
        TODO: DESCRIBE
        """
        if len(buf) == 0:
            return
        quoted = eod is not None and (eod[0] == "'" or eod[0] == '"')
        out.append(ShellFragment(''.join(buf), start_line_number,
                             ShellFragmentType.HEREDOC if state == ExtractorState.BTEQ else ShellFragmentType.SHELL,
                             quoted))
        buf.clear()

    def replace_command(self, line: str):
        if self.command_replace_from is None or self.command_replace_to is None:
            return line
        return line.replace(self.command_replace_from, self.command_replace_to, 1)

    def read_fragments(self, input_text: str) -> List[ShellFragment]:
        """
        TODO: DESCRIBE
        """
        out: List[ShellFragment] = []
        buf: List[str] = []
        eod: Optional[str] = None
        unquoted_eod: Optional[str] = None # with leading and trailing ' "" removed, if any
        state: ExtractorState = ExtractorState.SHELL
        line_number = 0
        start_line_number = 0
        for line in input_text.splitlines():
            logger.debug("state=%s, line_number=%d, line=%s, buf=%s", state, line_number, line, buf)
            if state == ExtractorState.SHELL:
                # strip off the text after #. This should be mostly GOOD.
                non_comment = line.split('#')[0]
                match = self.pattern.search(non_comment)
                logger.debug("non_comment=%s, match=%s", non_comment, match)
                if match:
                    eod = match.group(1)
                    unquoted_eod = eod[1:len(eod) - 1] if eod[0] == "'" or eod[0] == '"' else eod
                    # include the begin HEREDOC ; there may be comments
                    buf.extend([self.replace_command(line), '\n'])
                    self.move_fragment(out, buf, start_line_number, ExtractorState.SHELL, eod)
                    # prepare for Next ...
                    state = ExtractorState.BTEQ
                    start_line_number = line_number + 1
                else:
                    buf.extend([line, '\n'])
            else:
                # TODO: This needs to strip leading tabs in the - case.
                # TODO: equals eod is probably wrong
                logger.debug("unquoted_eod=%s, eod=%s", unquoted_eod, eod);
                if line == unquoted_eod or line == eod:
                    self.move_fragment(out, buf, start_line_number, ExtractorState.BTEQ, eod)
                    eod = None
                    state = ExtractorState.SHELL
                    start_line_number = line_number
                    # include the end of HEREDOC
                    buf.extend([line, '\n'])
                else:
                    buf.extend([line, '\n'])
            line_number += 1
        if state == ExtractorState.BTEQ:
            logger.warning("Here-document terminates at EOF in input_text.")
        self.move_fragment(out, buf, start_line_number, state, eod)
        return out
