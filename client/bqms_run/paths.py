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
"""Dataclass that contains project paths."""
import pathlib
from dataclasses import dataclass
from dataclasses import fields as dataclass_fields
from functools import singledispatch
from typing import Iterable, Iterator, Mapping, Optional, Tuple, Union

from cloudpathlib import CloudPath, GSPath
from cloudpathlib.anypath import to_anypath
from marshmallow import Schema, ValidationError, fields, post_load

Path = Union[CloudPath, pathlib.Path]


class _PathsSchema(Schema):
    """Schema and validator for Paths."""

    input_path = fields.Method(required=True, deserialize="_deserialize_directory")
    preprocessed_path = fields.Method(
        required=True, deserialize="_deserialize_gcs_directory_force_empty"
    )
    translated_path = fields.Method(
        required=True, deserialize="_deserialize_gcs_directory_force_empty"
    )
    postprocessed_path = fields.Method(
        required=True, deserialize="_deserialize_directory_force_empty"
    )
    config_path = fields.Method(deserialize="_deserialize_file")
    macro_mapping_path = fields.Method(deserialize="_deserialize_file")
    object_name_mapping_path = fields.Method(deserialize="_deserialize_file")

    @staticmethod
    def _deserialize_file(obj: str) -> Path:
        try:
            path = to_anypath(obj).resolve()
        except Exception as err:
            raise ValidationError(f"Invalid path: {err}.") from err
        if not path.is_file():
            raise ValidationError(f"Path must be a file: {path.as_uri()}.")
        return path

    @staticmethod
    def _deserialize_directory(obj: str) -> Path:
        try:
            path = to_anypath(obj).resolve()
        except Exception as err:
            raise ValidationError(f"Invalid path: {err}.") from err
        if not path.is_dir():
            raise ValidationError(f"Path must be a directory: {path.as_uri()}.")
        return path

    @classmethod
    def _rmtree(cls, path: Path) -> None:
        for child in path.glob("*"):
            if child.is_file():
                child.unlink()
            else:
                cls._rmtree(child)
        path.rmdir()

    @classmethod
    def _directory_force_empty(cls, path: Path) -> None:
        if path.exists() and path.is_dir():
            cls._rmtree(path)
        if path.exists() and path.is_file():
            path.unlink(missing_ok=True)
        path.mkdir()

    @classmethod
    def _deserialize_directory_force_empty(cls, obj: str) -> Path:
        try:
            path = to_anypath(obj).resolve()
        except Exception as err:
            raise ValidationError(f"Invalid path: {err}.") from err
        cls._directory_force_empty(path)
        return path

    @classmethod
    def _deserialize_gcs_directory_force_empty(cls, obj: str) -> GSPath:
        try:
            path = GSPath(obj)
        except Exception as err:
            raise ValidationError(f"Invalid path: {err}.") from err
        cls._directory_force_empty(path)
        return path

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return Paths(**data)


@dataclass
class Paths:
    """Project paths.

    Example:
        .. code-block::

            import os

            paths = Paths.from_mapping({
                "input_path": os.getenv("BQMS_INPUT_PATH"),
                "preprocessed_path": os.getenv("BQMS_PREPROCESSED_PATH"),
                "translated_path": os.getenv("BQMS_TRANSLATED_PATH"),
                "postprocessed_path": os.getenv("BQMS_POSTPROCESSED_PATH"),
                "config_path": os.getenv("BQMS_CONFIG_PATH"),
                "macro_mapping_path": os.getenv("BQMS_MACRO_MAPPING_PATH"),
                "object_name_mapping_path": os.getenv(
                    "BQMS_OBJECT_NAME_MAPPING_PATH"
                ),
            })

    Attributes:
        input_path: A bqms_run.paths.Path where input to be translated is located.
        preprocessed_path: A cloudpathlib.GSPath where preprocessed input to be
            translated is written.
        translated_path: A cloudpathlib.GSPath where translated output is written.
        postprocessed_path: A bqms_run.paths.Path where postprocessed translated
            output is written.
        config_path: A bqms_run.paths.Path where the translation config is located.
        macro_mapping_path: An optional bqms_run.paths.Path where the macro mapping
            is located.
        object_name_mapping_path: An optional bqms_run.paths.Path where the object
            name mapping is located.
    """

    input_path: Path
    preprocessed_path: GSPath
    translated_path: GSPath
    postprocessed_path: Path
    config_path: Path
    macro_mapping_path: Optional[Path] = None
    object_name_mapping_path: Optional[Path] = None

    @staticmethod
    def from_mapping(mapping: Mapping[str, object]) -> "Paths":
        """Factory method for creating a Paths instance from a Mapping.

        Args:
            mapping: A Mapping of the form shown in the class-level example.

        Returns:
            A Paths instance.
        """
        paths: Paths = _PathsSchema().load(mapping)
        return paths

    def __iter__(self) -> Iterator[Tuple[str, Path]]:
        for field in dataclass_fields(self.__class__):
            yield field.name, getattr(self, field.name)


# We use functools.singledispatch here instead of inheritance because Path
# objects are often created by third parties using the Path constructor directly
# instead of type(self).
@singledispatch
def iterdirfiles(path: Path) -> Iterable[Path]:
    for child_path in path.iterdir():
        if child_path.is_dir():
            yield from iterdirfiles(child_path)
        else:
            yield child_path


@iterdirfiles.register
def _(path: CloudPath) -> Iterable[CloudPath]:
    for child_path, is_dir in path.client._list_dir(  # pylint: disable=protected-access
        path, recursive=True
    ):
        if not is_dir and child_path != path:
            yield child_path
