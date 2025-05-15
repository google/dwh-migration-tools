# Copyright 2025 Google LLC
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
from collections import defaultdict
import re
from typing import Any, Dict, List
import yaml
import json

from jinja2 import Environment, FileSystemLoader, select_autoescape


class GCSPath:
  """Represents a Google Cloud Storage object."""

  def __init__(self, bucket_name: str, object_name: str) -> None:
    """Initializes a GcsPath object.

    Args:
        bucket_name (str): The bucket name.
        object_name (str): The object name.
    """
    self.bucket_name = bucket_name
    self.object_name = object_name


def aggregate_permissions(
    permissions: List[Dict],
) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
    """Aggregates permissions into a nested dictionary.

    Args:
        file_path (str): Path to the YAML file containing permissions.

    Returns:
        dict: A nested dictionary representing permissions. The structure is:
            bucket -> folder -> role -> list of principals
    """
    bucket_folder_role_to_permission = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    for permission in permissions:
        gcs_object = parse_gcs_path(permission["resourcePath"])
        role = permission["role"]
        principal = permission["principal"]
        bucket_folder_role_to_permission[gcs_object.bucket_name][
            gcs_object.object_name
        ][role].append(principal)
    return bucket_folder_role_to_permission


def parse_gcs_path(path: str) -> GCSPath:
    """Parses a Google Cloud Storage path into bucket and folder components.

    Args:
        path (str): The Google Cloud Storage path.

    Returns:
        GcsPath: A GcsPath object containing the bucket name and object name.

    Raises:
        ValueError: If the path is invalid.
    """
    match = re.match(r"^gs://([^/]+)/(.*)$", path)
    if not match:
        raise ValueError(f"Invalid GCS path: {path}")
    return GCSPath(*match.groups())


def load_yaml_file(path: str) -> Any:
    with open(path, "r") as file:
        return yaml.safe_load(file)


def create_json_file(path: str, json_config: Dict) -> None:
    with open(path, "w") as f:
        json.dump(json_config, f, indent=2)


def create_terraform_template(path: str, json_config: Dict) -> None:
  template_engine = Environment(
      loader=FileSystemLoader("tf_generator/templates"),
      autoescape=select_autoescape(),
  )
  with open(path, "w") as f:
      template = template_engine.get_template("tf_template.tf")
      f.write(template.render(json_config))