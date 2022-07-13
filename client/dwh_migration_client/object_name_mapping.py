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
"""Parses the object name mapping json file into ObjectNameMappingList."""

import json
import logging
from pprint import pformat

from google.cloud.bigquery_migration_v2 import (
    NameMappingKey,
    NameMappingValue,
    ObjectNameMapping,
    ObjectNameMappingList,
)
from marshmallow import Schema, ValidationError, fields, post_load, validate


class NameMappingKeySchema(Schema):
    """Schema and data validator for NameMappingKey."""

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


class NameMappingValueSchema(Schema):
    """Schema and data validator for NameMappingValue."""

    database = fields.String()
    schema = fields.String()
    relation = fields.String()
    attribute = fields.String()

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return NameMappingValue(**data)


class ObjectNameMappingSchema(Schema):
    """Schema and data validator for ObjectNameMapping."""

    source = fields.Nested(NameMappingKeySchema, required=True)
    target = fields.Nested(NameMappingValueSchema, required=True)

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return ObjectNameMapping(**data)


class ObjectNameMappingListSchema(Schema):
    """Schema and data validator for ObjectNameMappingList."""

    name_map = fields.List(fields.Nested(ObjectNameMappingSchema), required=True)

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return ObjectNameMappingList(**data)


def parse(object_name_mapping_file_path: str) -> ObjectNameMappingList:
    """Parses the object name mapping json file into ObjectNameMappingList."""
    logging.info("Parsing object name mapping file: %s.", object_name_mapping_file_path)
    with open(object_name_mapping_file_path, encoding="utf-8") as file:
        data = json.load(file)
    try:
        object_name_mapping_list: ObjectNameMappingList = (
            ObjectNameMappingListSchema().load(data)
        )
    except ValidationError as error:
        logging.error(
            "Invalid object name mapping file: %s: %s.",
            object_name_mapping_file_path,
            error,
        )
        raise
    logging.info(
        "Finished parsing object name mapping file: %s:\n%s",
        object_name_mapping_file_path,
        pformat(object_name_mapping_list),
    )
    return object_name_mapping_list
