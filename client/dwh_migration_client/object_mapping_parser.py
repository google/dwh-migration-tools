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
        self.__json_file_path = json_file_path

    __NAME_MAP_FIELD = "name_map"
    __SOURCE_FIELD = "source"
    __TARGET_FIELD = "target"
    __TYPE_FIELD = "type"
    __DB_FIELD = "database"
    __SCHEMA_FIELD = "schema"
    __RELATION_FIELD = "relation"
    __ATTRIBUTE_FIELD = "attribute"

    __SUPPORTED_TYPES = {
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
        print('Start parsing object name mapping from "%s"...' % self.__json_file_path)
        self.__validate_onm_file()
        onm_list = ObjectNameMappingList()
        with open(self.__json_file_path, encoding="utf-8") as file:
            data = json.load(file)
        for name_map in data[self.__NAME_MAP_FIELD]:
            assert self.__SOURCE_FIELD in name_map, (
                'Invalid object name mapping: can\'t find a "%s" field in "%s". '
                'Instead we got "%s".'
                % (self.__SOURCE_FIELD, self.__json_file_path, name_map)
            )
            assert self.__TARGET_FIELD in name_map, (
                'Invalid object name mapping: can\'t find a "%s" field in "%s". '
                'Instead we got "%s".'
                % (self.__TARGET_FIELD, self.__json_file_path, name_map)
            )
            onm = ObjectNameMapping()
            onm.source = self.__parse_source(name_map[self.__SOURCE_FIELD])
            onm.target = self.__parse_target(name_map[self.__TARGET_FIELD])
            onm_list.name_map.append(onm)
        return onm_list

    def __parse_source(self, source_data: Dict[str, str]) -> NameMappingKey:
        name_mapping_key = NameMappingKey()
        if self.__TYPE_FIELD in source_data:
            assert source_data[self.__TYPE_FIELD] in self.__SUPPORTED_TYPES, (
                'The source type of name mapping "%s" in file "%s" is not one of'
                'the supported types: "%s"'
                % (
                    source_data[self.__TYPE_FIELD],
                    self.__json_file_path,
                    self.__SUPPORTED_TYPES,
                )
            )
            name_mapping_key.type_ = source_data[self.__TYPE_FIELD]
        if self.__DB_FIELD in source_data:
            name_mapping_key.database = source_data[self.__DB_FIELD]
        if self.__SCHEMA_FIELD in source_data:
            name_mapping_key.schema = source_data[self.__SCHEMA_FIELD]
        if self.__RELATION_FIELD in source_data:
            name_mapping_key.relation = source_data[self.__RELATION_FIELD]
        if self.__ATTRIBUTE_FIELD in source_data:
            name_mapping_key.attribute = source_data[self.__ATTRIBUTE_FIELD]
        return name_mapping_key

    def __parse_target(self, target_data: Dict[str, str]) -> NameMappingValue:
        name_mapping_value = NameMappingValue()
        if self.__DB_FIELD in target_data:
            name_mapping_value.database = target_data[self.__DB_FIELD]
        if self.__SCHEMA_FIELD in target_data:
            name_mapping_value.schema = target_data[self.__SCHEMA_FIELD]
        if self.__RELATION_FIELD in target_data:
            name_mapping_value.relation = target_data[self.__RELATION_FIELD]
        if self.__ATTRIBUTE_FIELD in target_data:
            name_mapping_value.attribute = target_data[self.__ATTRIBUTE_FIELD]
        return name_mapping_value

    def __validate_onm_file(self) -> None:
        assert os.path.isfile(self.__json_file_path), (
            'Can\'t find a file at "%s".' % self.__json_file_path
        )
