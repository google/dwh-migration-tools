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
"""Parses the config file into a Config object."""

import logging
from dataclasses import asdict, dataclass
from pprint import pformat
from typing import List, Dict, Optional

import yaml
from marshmallow import Schema, ValidationError, fields, post_load
from yaml.loader import SafeLoader

from dwh_migration_client.translation_type import TranslationType


@dataclass
class GcpConfig:
    project_number: str
    gcs_bucket: str


class GcpConfigSchema(Schema):
    """Schema and data validator for GcpConfig."""

    project_number = fields.String(required=True)
    gcs_bucket = fields.String(required=True)

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return GcpConfig(**data)


@dataclass
class TranslationConfig:
    translation_type: TranslationType
    location: str
    default_database: Optional[str]
    schema_search_path: Optional[List[str]]
    clean_up_tmp_files: bool
    source_target_location_mapping: Optional[Dict[str, str]]


class TranslationConfigSchema(Schema):
    """Schema and data validator for TranslationConfig."""

    translation_type = fields.Method(
        required=True, deserialize="_deserialize_translation_type"
    )

    @staticmethod
    def _deserialize_translation_type(obj: str) -> TranslationType:
        for member in TranslationType:
            if member.name == obj:
                return member
        raise ValidationError(f"{obj} is not a valid translation type.")

    location = fields.String(required=True)
    default_database = fields.String(load_default=None)
    schema_search_path = fields.List(fields.String(), load_default=None)
    clean_up_tmp_files = fields.Boolean(load_default=True)
    source_target_location_mapping = fields.Dict(
        keys=fields.String(), values=fields.String, load_default=None
    )

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return TranslationConfig(**data)


@dataclass
class Config:
    gcp_settings: GcpConfig
    translation_config: TranslationConfig


class ConfigSchema(Schema):
    """Schema and data validator for Config."""

    gcp_settings = fields.Nested(GcpConfigSchema, required=True)
    translation_config = fields.Nested(TranslationConfigSchema, required=True)

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return Config(**data)


def parse(config_file_path: str) -> Config:
    """Parses the config file into a Config object.

    Return:
        Config object.
    """
    logging.info("Parsing config file: %s.", config_file_path)
    with open(config_file_path, encoding="utf-8") as file:
        data = yaml.load(file, Loader=SafeLoader)
    try:
        config: Config = ConfigSchema().load(data)
    except ValidationError as error:
        logging.error("Invalid config file: %s: %s.", config_file_path, error)
        raise
    logging.info(
        "Finished parsing config file: %s:\n%s.",
        config_file_path,
        pformat(asdict(config)),
    )
    return config
