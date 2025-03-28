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
import os
import sys
import ruamel.yaml
from ruamel.yaml.scalarstring import DoubleQuotedScalarString as dq


# generate a table object based on the following yaml example:
# sourceName: "__DEFAULT_DATABASE__.db1.tab1"
# targetName: "777.db1.tab1"
# sourceLocations:
# - "hdfs://dummy-hdfs-location/user/hive/warehouse/db1.db/tab1"
# targetLocations:
# - "gs://dummy-gcs-location/db1/tab1"
# tab1 should be replaced with tab + str(i)
# db1 should be replaced with db + str(db_num)
def generate_table(i, db_num):
  return {
      "sourceName": dq(
          "__DEFAULT_DATABASE__.db" + str(db_num) + ".tab" + str(i)
      ),
      "targetName": dq("777.db" + str(db_num) + ".tab" + str(i)),
      "sourceLocations": [
          dq("hdfs://dummy-hdfs-location/db" + str(db_num) + "/tab" + str(i))
      ],
      "targetLocations": [
          dq("gs://dummy-gcs-location/db" + str(db_num) + "/tab" + str(i))
      ],
  }


def main(num_tables):
  NUM_DBS = 3

  yaml = ruamel.yaml.YAML()
  for db_num in range(NUM_DBS):
    for i in range(num_tables):
      filename = (
          "tables/dts/default_database/db"
          + str(db_num)
          + "/tab"
          + str(i)
          + ".yaml"
      )
      os.makedirs(os.path.dirname(filename), exist_ok=True)
      with open(filename, "w") as f:
        yaml.dump(generate_table(i, db_num), f)


if __name__ == "__main__":
  if len(sys.argv) != 2:
    print("Usage: python3 load_test_table_generator.py <num_tables>")
    sys.exit(1)
  main(int(sys.argv[1]))

