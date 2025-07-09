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
import sys
import ruamel.yaml
from ruamel.yaml.scalarstring import DoubleQuotedScalarString as dq


def create_one_to_one_principal_ruleset_of_type(
    principal_type, num_of_principals
):
  mappings = []
  for i in range(num_of_principals):
    mappings.append({
        "when": dq(
            principal_type + ".name == '" + principal_type + str(i) + "'"
        ),
        "map": {
            "email_address": {
                "expression": dq(
                    "'migrated-' + " + principal_type + ".name + '@google.com'"
                )
            }
        },
    })
  mappings.append({"skip": "true"})
  return mappings


def create_one_to_one_ranger_principal_ruleset(groups, users, roles):
  return {
      "user_rules": create_one_to_one_principal_ruleset_of_type("user", users),
      "group_rules": create_one_to_one_principal_ruleset_of_type(
          "group", groups
      ),
      "role_rules": create_one_to_one_principal_ruleset_of_type("role", roles),
  }


def create_one_to_one_hdfs_principal_ruleset(groups, users):
  return {
      "user_rules": create_one_to_one_principal_ruleset_of_type("user", users),
      "group_rules": create_one_to_one_principal_ruleset_of_type(
          "group", groups
      ),
      "other_rules": [{"skip": "true"}]
  }


def create_full_one_to_one_principal_ruleset(groups, users, roles):
  return {
      "ranger": create_one_to_one_ranger_principal_ruleset(
          groups, users, roles
      ),
      "hdfs": create_one_to_one_hdfs_principal_ruleset(groups, users),
  }


def create_regex_principal_ruleset_of_type(principal_type):
  mappings = []
  for i in range(10):
    mappings.append({
        "when": dq(
            "matches("
            + principal_type
            + ".name, '"
            + principal_type
            + str(i)
            + "[0-9]*')"
        ),
        "map": {
            "email_address": {
                "expression": dq(
                    "'migrated-' + " + principal_type + ".name + '@google.com'"
                )
            }
        },
    })
  mappings.append({"skip": "true"})
  return mappings


def create_regex_ranger_principal_ruleset():
  return {
      "user_rules": create_regex_principal_ruleset_of_type("user"),
      "group_rules": create_regex_principal_ruleset_of_type("group"),
      "role_rules": create_regex_principal_ruleset_of_type("role"),
  }


def create_regex_hdfs_principal_ruleset():
  return {
      "user_rules": create_regex_principal_ruleset_of_type("user"),
      "group_rules": create_regex_principal_ruleset_of_type("group"),
      "other_rules": [{"skip": "true"}]
  }


def create_full_regex_principal_ruleset():
  return {
      "ranger": create_regex_ranger_principal_ruleset(),
      "hdfs": create_regex_hdfs_principal_ruleset(),
  }


def main(num_users, num_groups, num_roles):

  yaml = ruamel.yaml.YAML()

  principal_ruleset = create_full_one_to_one_principal_ruleset(
      num_groups, num_users, num_roles
  )
  # save the principal mapping in a yaml file named principal_mapping.yaml
  with open("principal-ruleset.yaml", "w") as f:
    yaml.dump(principal_ruleset, f)

  regex_principal_ruleset = create_full_regex_principal_ruleset()
  # save the principal mapping in a yaml file named principal_mapping.yaml
  with open("regex-principal-ruleset.yaml", "w") as f:
    yaml.dump(regex_principal_ruleset, f)


if __name__ == "__main__":
  if len(sys.argv) != 4:
    print(
        "Usage: python3 load_test_principal_ruleset_generator.py <num_users>"
        " <num_groups> <num_roles>"
    )
    sys.exit(1)
  main(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))

