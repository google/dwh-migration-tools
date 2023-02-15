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

"""KSH processing unit tests."""
import logging
import re
from pathlib import Path

from pytest import raises

from bqms_run.encoding import EncodingDetector
from bqms_run.hooks import preprocess as preprocess_hook
from bqms_run.ksh import KshExtractor

logger = logging.getLogger(__name__)


def collapse_whitespace(strs: [str]):
    """
    Test utility method to make assertion expectations easier to read.
    """
    return list(map(lambda s: re.sub(r"\s+", " ", s), strs))


def test_invalid_pattern():
    with raises(ValueError):
        KshExtractor("foo(bar)baz")  # introduces an extra capturing group


def test_commented_ksh():
    with open("tests/unit/test_ksh.input/commented.ksh", "rb") as file:
        data = file.read()
        encoding = EncodingDetector().detect(data)
        assert "ISO-8859-1" == encoding
        fragments = KshExtractor("bteq").read_fragments(EncodingDetector().decode(data))
        assert ["select 1\n"] == KshExtractor.filter_heredoc_sql_texts(fragments)


def test_empty_ksh():
    with open("tests/unit/test_ksh.input/empty.ksh", "rb") as file:
        data = file.read()
        encoding = EncodingDetector().detect(data)
        assert "ISO-8859-1" == encoding
        fragments = KshExtractor("bteq").read_fragments(EncodingDetector().decode(data))
        assert not KshExtractor.filter_heredoc_sql_texts(fragments)


def test_no_prefix_ksh():
    with open("tests/unit/test_ksh.input/no-prefix.ksh", "rb") as file:
        data = file.read()
        encoding = EncodingDetector().detect(data)
        assert "ISO-8859-1" == encoding
        fragments = KshExtractor("bteq").read_fragments(EncodingDetector().decode(data))
        assert ["select 1 + 1;\n"] == KshExtractor.filter_heredoc_sql_texts(fragments)


def test_simple_ksh():
    with open("tests/unit/test_ksh.input/simple.ksh", "rb") as file:
        data = file.read()
        encoding = EncodingDetector().detect(data)
        assert "ISO-8859-1" == encoding
        fragments = KshExtractor("bteq").read_fragments(EncodingDetector().decode(data))
        assert [
            "SELECT 1 ; select '$Something' ; select '`backtick`'; select '\\\\esc'; ",
            "SELECT 2 ; select '$Something' ; select '`backtick`'; select '\\\\esc'; ",
            "select '$Something' ; select '`backtick`'; select '\\\\esc'; ",
        ] == collapse_whitespace(KshExtractor.filter_heredoc_sql_texts(fragments))


def test_with_flags_ksh():
    with open("tests/unit/test_ksh.input/with-flags.ksh", "rb") as file:
        data = file.read()
        encoding = EncodingDetector().detect(data)
        assert "ISO-8859-1" == encoding
        fragments = KshExtractor("bteq").read_fragments(EncodingDetector().decode(data))
        assert [
            " .LOGON ${LOGON_STRING}; .export data file=${OUTDIR}/file.tmp; select trim(((date - ${DAYIND}(integer)) (integer))(char(20)))(char(7)); .export reset; .quit errorcode; ",  # pylint: disable=line-too-long
            " .LOGON ${LOGON_STRING}; select 'uninteresting-lot-of-sql'; .if errorcode != 0 then .quit errorcode .quit errorcode; ",  # pylint: disable=line-too-long
        ] == collapse_whitespace(KshExtractor.filter_heredoc_sql_texts(fragments))


def test_with_prefix_ksh():
    with open("tests/unit/test_ksh.input/with-prefix.ksh", "rb") as file:
        data = file.read()
        encoding = EncodingDetector().detect(data)
        assert "ISO-8859-1" == encoding
        fragments = KshExtractor("bteq").read_fragments(EncodingDetector().decode(data))
        assert [
            ".maxerror 1; .run file = $SCRIPTS_DIR/ETL_USER_$test_environment.login; delete stg.foo; ;insert into stg.foo SELECT A.foo, A.bar, A.baz, cast(AVG(b.quam) as decimal) as quix, cast(avg(cast(A.qib as date) - cast(A.zim as date)) as decimal) as zob, SUM(A.jibber) as jabber FROM tb.foo a,sys_calendar.calendar b WHERE a.qib = b.quam AND A.zif = 1 AND A.zaf = 1 AND A.cor >= (select year_of_calendar-1||'0101' from sys_calendar.calendar where calendar_date = current_date) AND A.jif = 1 AND A.foo IS NOT NULL AND A.bar IS NOT NULL AND A.quam IS NOT NULL GROUP BY A.foo,A.bar,A.baz; .quit; "  # pylint: disable=line-too-long
        ] == collapse_whitespace(KshExtractor.filter_heredoc_sql_texts(fragments))


def test_iso_8859_1_ksh():
    with open("tests/unit/test_ksh.input/iso_8859_1.ksh", "rb") as file:
        data = file.read()
        encoding = EncodingDetector().detect(data)
        assert "ISO-8859-1" == encoding
        fragments = KshExtractor("bteq").read_fragments(EncodingDetector().decode(data))
        assert ["SELECT 'ä,ö,ü' ; "] == collapse_whitespace(
            KshExtractor.filter_heredoc_sql_texts(fragments)
        )


def test_utf8_ksh():
    with open("tests/unit/test_ksh.input/utf8.ksh", "rb") as file:
        data = file.read()
        encoding = EncodingDetector().detect(data)
        assert "UTF-8" == encoding
        fragments = KshExtractor("bteq").read_fragments(EncodingDetector().decode(data))
        assert ["SELECT '🌩' ; "] == collapse_whitespace(
            KshExtractor.filter_heredoc_sql_texts(fragments)
        )


def test_preprocess_hook_includes_ksh_extractor():
    """
    Asserts that KSH/heredoc extraction runs when the preprocess
    hook is invoked.
    """
    contents = """#!/bin/ksh
## A Very simple test.
bteq  <<EOF
SELECT 123, 'foo', 456 from bar;
EOF
echo Trying another select.
"""
    expected = "SELECT 123, 'foo', 456 from bar;\n"
    preprocessed = preprocess_hook(Path("foo/bar.ksh"), contents)
    assert expected == preprocessed
