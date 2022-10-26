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
"""Schemas and validators for object name mapping."""
from typing import Mapping

from google.cloud.bigquery_migration_v2 import (
    NameMappingKey,
    NameMappingValue,
    ObjectNameMapping,
    ObjectNameMappingList,
)
from marshmallow import Schema, fields, post_load, validate


class _NameMappingKeySchema(Schema):
    """Schema and validator for NameMappingKey."""

    type = fields.String(
        required=True,
        validate=validate.OneOf(
            (
                "DATABASE",
                "SCHEMA",
                "RELATION",
                "ATTRIBUTE",
                "RELATION_ALIAS",
                "ATTRIBUTE_ALIAS",
                "FUNCTION",
            )
        ),
    )
    database = fields.String()
    schema = fields.String()
    relation = fields.String()
    attribute = fields.String()

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return NameMappingKey(**data)


class _NameMappingValueSchema(Schema):
    """Schema and validator for NameMappingValue."""

    database = fields.String()
    schema = fields.String()
    relation = fields.String()
    attribute = fields.String()

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return NameMappingValue(**data)


class _ObjectNameMappingSchema(Schema):
    """Schema and validator for ObjectNameMapping."""

    source = fields.Nested(_NameMappingKeySchema, required=True)
    target = fields.Nested(_NameMappingValueSchema, required=True)

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return ObjectNameMapping(**data)


class ObjectNameMappingListSchema(Schema):
    """Schema and validator for ObjectNameMappingList."""

    name_map = fields.List(fields.Nested(_ObjectNameMappingSchema), required=True)

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return ObjectNameMappingList(**data)

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> ObjectNameMappingList:
        """Factory method for creating a ObjectNameMappingList from a Mapping.

        Args:
            mapping: A Mapping similar to
                examples/teradata/sql/config/object_name_mapping.json.

        Returns:
            A ObjectNameMappingList instance.
        """
        object_name_mapping_list: ObjectNameMappingList = cls().load(mapping)
        return object_name_mapping_list
