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
"""A parser for the object name mapping json file."""

import json
import logging
import os
from typing import Dict

from google.cloud.bigquery_migration_v2 import (
    NameMappingKey,
    NameMappingValue,
    ObjectNameMapping,
    ObjectNameMappingList,
)


class ObjectMappingParser:  # pylint: disable=too-few-public-methods
    """A parser for the object name mapping json file."""

    def __init__(self, json_file_path: str) -> None:
        self._json_file_path = json_file_path

    _NAME_MAP_FIELD = "name_map"
    _SOURCE_FIELD = "source"
    _TARGET_FIELD = "target"
    _TYPE_FIELD = "type"
    _DB_FIELD = "database"
    _SCHEMA_FIELD = "schema"
    _RELATION_FIELD = "relation"
    _ATTRIBUTE_FIELD = "attribute"

    _SUPPORTED_TYPES = {
        "DATABASE",
        "SCHEMA",
        "RELATION",
        "ATTRIBUTE",
        "RELATION_ALIAS",
        "ATTRIBUTE_ALIAS",
        "FUNCTION",
    }

    def get_name_mapping_list(self) -> ObjectNameMappingList:
        """Parses the object name mapping json file into ObjectNameMappingList."""
        logging.info(
            'Start parsing object name mapping from "%s"...', self._json_file_path
        )
        self._validate_onm_file()
        onm_list = ObjectNameMappingList()
        with open(self._json_file_path, encoding="utf-8") as file:
            data = json.load(file)
        for name_map in data[self._NAME_MAP_FIELD]:
            assert self._SOURCE_FIELD in name_map, (
                f'Invalid object name mapping: can\'t find a "{self._SOURCE_FIELD}" '
                f'field in "{self._json_file_path}". '
                f'Instead we got "{name_map}".'
            )
            assert self._TARGET_FIELD in name_map, (
                f'Invalid object name mapping: can\'t find a "{self._TARGET_FIELD}" '
                f'field in "{self._json_file_path}". '
                f'Instead we got "{name_map}".'
            )
            onm = ObjectNameMapping()
            onm.source = self._parse_source(name_map[self._SOURCE_FIELD])
            onm.target = self._parse_target(name_map[self._TARGET_FIELD])
            onm_list.name_map.append(onm)
        return onm_list

    def _parse_source(self, source_data: Dict[str, str]) -> NameMappingKey:
        name_mapping_key = NameMappingKey()
        if self._TYPE_FIELD in source_data:
            assert source_data[self._TYPE_FIELD] in self._SUPPORTED_TYPES, (
                f'The source type of name mapping "{source_data[self._TYPE_FIELD]}" '
                f'in file "{self._json_file_path}" is not one of'
                f'the supported types: "{self._SUPPORTED_TYPES}"'
            )
            name_mapping_key.type_ = source_data[self._TYPE_FIELD]
        if self._DB_FIELD in source_data:
            name_mapping_key.database = source_data[self._DB_FIELD]
        if self._SCHEMA_FIELD in source_data:
            name_mapping_key.schema = source_data[self._SCHEMA_FIELD]
        if self._RELATION_FIELD in source_data:
            name_mapping_key.relation = source_data[self._RELATION_FIELD]
        if self._ATTRIBUTE_FIELD in source_data:
            name_mapping_key.attribute = source_data[self._ATTRIBUTE_FIELD]
        return name_mapping_key

    def _parse_target(self, target_data: Dict[str, str]) -> NameMappingValue:
        name_mapping_value = NameMappingValue()
        if self._DB_FIELD in target_data:
            name_mapping_value.database = target_data[self._DB_FIELD]
        if self._SCHEMA_FIELD in target_data:
            name_mapping_value.schema = target_data[self._SCHEMA_FIELD]
        if self._RELATION_FIELD in target_data:
            name_mapping_value.relation = target_data[self._RELATION_FIELD]
        if self._ATTRIBUTE_FIELD in target_data:
            name_mapping_value.attribute = target_data[self._ATTRIBUTE_FIELD]
        return name_mapping_value

    def _validate_onm_file(self) -> None:
        assert os.path.isfile(
            self._json_file_path
        ), f'Can\'t find a file at "{self._json_file_path}".'
