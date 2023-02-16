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

import icu

logger = logging.getLogger(__name__)


class EncodingDetector:
    """
    An encoding detector.
    """

    def detect(self, data: bytes) -> str:
        """
        Detect the encoding of the provided bytes, return the encoding name.
        """
        encoding = icu.CharsetDetector(data).detect().getName()
        if not isinstance(encoding, str):
            return "utf-8"
        return encoding

    def decode(self, data: bytes) -> str:
        """
        Detect the encoding of the provided bytes, then decode them to string.
        """
        encoding = self.detect(data)
        logger.debug("Detected encoding: %s", encoding)
        return data.decode(encoding)
