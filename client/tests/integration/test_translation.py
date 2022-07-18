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
"""End to end translation test."""
from dwh_migration_client.main import parse_args, start_translation


def test_translation():
    """End to end translation test."""
    args = parse_args(
        [
            "--config",
            "config.yaml",
            "--input",
            "input",
            "--output",
            "output",
            "-o",
            "object_mapping.json",
            "-m",
            "macros.yaml",
        ]
    )
    start_translation(args)
