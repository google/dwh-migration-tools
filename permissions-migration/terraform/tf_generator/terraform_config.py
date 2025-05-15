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
from copy import deepcopy
from typing import Dict, List


class TerraformConfig:
  """Represents a Terraform configuration for permission migration.

  In particular, it manages:
      - GCS buckets,
      - managed folders,
      - IAM permissions to the managed folders.
  """

  def __init__(self, location) -> None:
    """Initializes a TerraformConfig object.

    Args:
        location (str): The location for the Google Cloud Storage resources.
    """
    self._config = dict()
    self._location = location

  @property
  def config(self) -> Dict:
    return deepcopy(self._config)

  def build(
      self,
      bucket_folder_role_to_permission: Dict[
          str, Dict[str, Dict[str, List[str]]]
      ],
  ):
    """Creates the Terraform configuration based on the aggregated permissions.

    Args:
        bucket_folder_role_to_permission (dict): The nested dictionary of
          permissions.
    """
    for bucket_num, (bucket, folder_role_to_permission) in enumerate(
        bucket_folder_role_to_permission.items()
    ):
      bucket_internal_name = f"{bucket}{bucket_num}"
      self._add_bucket(bucket_internal_name, bucket, self._location)
      for folder_num, (folder, role_to_permission) in enumerate(
          folder_role_to_permission.items()
      ):
        encoded_folder = folder.replace('/', '_')
        folder_internal_name = f"{encoded_folder}{folder_num}"
        policy_internal_name = f"{bucket}_{encoded_folder}{folder_num}"
        self._add_managed_folder(
            folder_internal_name, bucket_internal_name, folder
        )
        self._add_iam_policy(policy_internal_name, role_to_permission)
        self._add_managed_folder_iam(folder_internal_name, policy_internal_name)

  def _add_bucket(self, internal_name: str, name: str, location: str):
    self._get_node(["resource", "google_storage_bucket"])[internal_name] = {
        "name": name,
        "location": location,
        "force_destroy": True,
        "uniform_bucket_level_access": True,
    }

  def _add_managed_folder(
      self, internal_name: str, bucket_internal_name: str, name: str
  ):
    if name == '' or name[-1] != '/':
      name = name + '/'
    self._get_node(["resource", "google_storage_managed_folder"])[
        internal_name
    ] = {
        "bucket": f"${{google_storage_bucket.{bucket_internal_name}.name}}",
        "name": name,
    }

  def _add_iam_policy(
      self, internal_name: str, role_to_members: Dict[str, List[str]]
  ):
    self._get_node(["data", "google_iam_policy"])[internal_name] = {
        "binding": [
            {"role": role, "members": members}
            for role, members in role_to_members.items()
        ]
    }

  def _add_managed_folder_iam(
      self, folder_internal_name: str, policy_internal_name: str
  ):
    self._get_node(["resource", "google_storage_managed_folder_iam_policy"])[
        policy_internal_name
    ] = {
        "bucket": (
            f"${{google_storage_managed_folder.{folder_internal_name}.bucket}}"
        ),
        "managed_folder": (
            f"${{google_storage_managed_folder.{folder_internal_name}.name}}"
        ),
        "policy_data": (
            f"${{data.google_iam_policy.{policy_internal_name}.policy_data}}"
        ),
    }

  def _get_node(self, hierarchy: List[str]) -> Dict:
    """Gets a nested node in the Terraform configuration dictionary.

    Args:
        hierarchy (list): A list of keys representing the hierarchy of the node.

    Returns:
        dict: The nested node in the configuration dictionary.
    """
    cur = self._config
    for part in hierarchy:
      if part not in cur:
        cur[part] = {}
      cur = cur[part]
    return cur