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
"""Encoding utilities."""

import logging

logger = logging.getLogger(__name__)


class EncodingDetector:
    """
    An encoding detector.
    """

    DEFAULT_ENCODING = "utf-8"
    has_logged_fallback_warning = False

    def detect(self, data: bytes) -> str:
        """
        Detect the encoding of the provided bytes, return the encoding name.
        """
        try:
            # pylint: disable-next=import-outside-toplevel
            import icu

            encoding = icu.CharsetDetector(data).detect().getName()
            if not isinstance(encoding, str):
                return EncodingDetector.DEFAULT_ENCODING
            return encoding
        # pylint: disable-next=broad-exception-caught
        except Exception as ex:
            # any ICU-related exceptions should not halt execution,
            # just assume UTF-8
            if not EncodingDetector.has_logged_fallback_warning:
                # only print a single per-process warning to avoid log noise
                EncodingDetector.has_logged_fallback_warning = True
                logger.warning(
                    # pylint: disable-next=line-too-long
                    "PyICU is either not available or misconfigured; assuming default encoding of %s (cause: %s)",
                    EncodingDetector.DEFAULT_ENCODING,
                    ex,
                )
            return EncodingDetector.DEFAULT_ENCODING

    def decode(self, data: bytes) -> str:
        """
        Detect the encoding of the provided bytes, then decode them to string.
        """
        encoding = self.detect(data)
        logger.debug("Detected encoding: %s", encoding)
        return data.decode(encoding)
