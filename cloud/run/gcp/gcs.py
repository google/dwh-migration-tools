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
"""Dataclass, schema and validator for GCS."""

import pathlib
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Optional

from google.cloud.storage.bucket import Bucket
from google.cloud.storage.client import Client
from marshmallow import Schema, ValidationError, fields, post_load


class _GCSSchema(Schema):
    """Schema and validator for GCS."""

    project = fields.String(required=True)

    _bucket = fields.String(required=True)

    input_path = fields.Method(required=True, deserialize="_deserialize_path")
    preprocessed_path = fields.Method(required=True, deserialize="_deserialize_path")
    translated_path = fields.Method(required=True, deserialize="_deserialize_path")
    postprocessed_path = fields.Method(required=True, deserialize="_deserialize_path")
    macro_mapping_path = fields.Method(deserialize="_deserialize_path")
    object_name_mapping_path = fields.Method(deserialize="_deserialize_path")

    @staticmethod
    def _deserialize_path(obj: str) -> pathlib.Path:
        try:
            path = pathlib.Path(obj)
        except Exception as err:
            raise ValidationError(f"Invalid path: {err}.") from err
        return path

    @post_load
    def build(self, data, **kwargs) -> "GCS":  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return GCS(**data)


@dataclass
class GCS:
    """Class for interacting with the GCS client library.

    Example:
        .. code-block::

            import os

            gcs = GCS.from_mapping({
                "project": "project_name",
                "bucket": os.getenv("BQMS_GCS_BUCKET"),
                "input_path": os.getenv("BQMS_GCS_INPUT_PATH"),
                "preprocessed_path": os.getenv("BQMS_GCS_PREPROCESSED_PATH"),
                "translated_path": os.getenv("BQMS_GCS_TRANSLATED_PATH"),
                "postprocessed_path": os.getenv("BQMS_GCS_POSTPROCESSED_PATH"),
                "macro_mapping_path": os.getenv("BQMS_GCS_MACRO_MAPPING_PATH"),
                "object_name_mapping_path": os.getenv(
                    "BQMS_GCS_OBJECT_NAME_MAPPING_PATH"
                ),
            })

            input_uri = gcs.uri(gcs.input_path)

            for input_blob in gcs.bucket.list_blobs(
                prefix=gcs.input_path.as_posix()
            ):
                ...

    Attributes:
        project: A string representing a GCP project.
        client: A potentially cached thread-local
            google.cloud.storage.client.Client used to interact with GCS.
        bucket: A google.cloud.storage.bucket.Bucket that is the root of
            input_path, preprocessed_path, translated_path, etc.
        input_path: A pathlib.Path representing the GCS bucket-relative path
            where input to be translated is located.
        preprocessed_path: A pathlib.Path representing the GCS bucket-relative
            path where preprocessed input to be translated is located.
        translated_path: A pathlib.Path representing the GCS bucket-relative
            path where translated output is located.
        postprocessed_path: A pathlib.Path representing the GCS bucket-relative
            path where postprocessed translated output is located.
        macro_mapping_path: An optional pathlib.Path representing the GCS bucket
            relative path where the macro mapping is located.
        object_name_mapping_path: An optional pathlib.Path representing the GCS
            bucket-relative path where the object name mapping is located.
    """

    project: str
    _bucket: str
    input_path: pathlib.Path
    preprocessed_path: pathlib.Path
    translated_path: pathlib.Path
    postprocessed_path: pathlib.Path
    macro_mapping_path: Optional[pathlib.Path] = None
    object_name_mapping_path: Optional[pathlib.Path] = None

    @staticmethod
    def from_mapping(mapping: Mapping[str, object]) -> "GCS":
        """Factory method for creating a GCS instance from a Mapping.

        Args:
            mapping: A Mapping of the form shown in the class-level example.

        Returns:
            A GCS instance.
        """
        gcs: GCS = _GCSSchema().load(mapping)
        return gcs

    _thread_local = threading.local()

    @property
    def client(self) -> Client:
        if getattr(self._thread_local, "client", None) is None:
            self._thread_local.client = Client(project=self.project)
        return self._thread_local.client

    @property
    def bucket(self) -> Bucket:
        return self.client.bucket(bucket_name=self._bucket)

    def uri(self, path: pathlib.Path) -> str:
        """Builds a GCS URI from a GCS path.

        Args:
            path: A pathlib.Path representing a GCS path.

        Returns:
            A string representing a GCS URI.
        """
        return f"gs://{self._bucket}/{path.as_posix()}"
