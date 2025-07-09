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
import argparse
import sys

from utils import aggregate_permissions, load_yaml_file, create_terraform_template
from terraform_config import TerraformConfig


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description="Generate Terraform configuration for GCS permissions."
  )
  parser.add_argument(
      "permissions_file", help="Path to the YAML file containing permissions."
  )
  parser.add_argument(
      "location", help="Location for the Google Cloud Storage resources."
  )
  parser.add_argument(
      "output",
      default="main.tf",
      nargs="?",
      help="Path to the output .tf.json file.",
  )
  args = parser.parse_args()

  try:
      yaml_raw_permissions = load_yaml_file(args.permissions_file)
      bucket_folder_role_to_permission = aggregate_permissions(yaml_raw_permissions["permissions"])
  except ValueError as e:
    print(e)
    sys.exit(1)

  terraform_config = TerraformConfig(args.location)
  terraform_config.build(bucket_folder_role_to_permission)
  create_terraform_template(args.output, terraform_config.config)
