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
"""Schema and validator for source environment."""
from typing import Mapping

from google.cloud import bigquery_migration_v2
from marshmallow import Schema, fields, post_load


class SourceEnvSchema(Schema):
    """Schema and validator for SourceEnv."""

    default_database = fields.String()
    schema_search_path = fields.List(fields.String())

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return bigquery_migration_v2.SourceEnv(**data)

    @classmethod
    def from_mapping(
        cls, mapping: Mapping[str, object]
    ) -> bigquery_migration_v2.SourceEnv:
        """Factory method for creating a SourceEnv from a Mapping.

        Args:
            mapping: A Mapping of the form:
                {"default_database": "foo", "schema_search_path": ["bar", "baz"]}.

        Returns:
            A SourceEnv instance.
        """
        source_env: bigquery_migration_v2.SourceEnv = cls().load(mapping)
        return source_env
