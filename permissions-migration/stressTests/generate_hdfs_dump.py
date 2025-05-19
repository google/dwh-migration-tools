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
import csv
import sys

# generate a list of strings with entriesbased on the following example csv header and sample line:
# Path,FileType,FileSize,Owner,Group,Permission,ModificationTime,FileCount,DirCount,StoragePolicy
# /db3/tab0,D,3,user0,group0,rwxrwx---,2024-09-26 11:43:24.877,1,0,0
# the first item is a path in the format /db3/tab[number]
# the user0 and group0 are parameters
# the rwxrwx--- block should be split into 3 blocks and be set as parameters
# the other items can be set to constants
def generate_hdfs_dump_line(
    table_number, user_number, group_number, user_rwx, group_rwx, other_rwx
):
  return {
      "Path": "/db2/tab" + str(table_number),
      "FileType": "D",
      "FileSize": "3",
      "Owner": "user" + str(user_number),
      "Group": "group" + str(group_number),
      "Permission": user_rwx + group_rwx + other_rwx,
      "ModificationTime": "2024-09-26 11:43:24.877",
      "FileCount": "1",
      "DirCount": "0",
      "StoragePolicy": "0",
  }


def generate_hdfs_dump(num_tables, num_users, num_groups):
  lines = []
  user_num = 0
  group_num = 0
  for i in range(num_tables):
    lines.append(
        generate_hdfs_dump_line(
            i, user_num % num_users, group_num % num_groups, "rwx", "rwx", "---"
        )
    )
    group_num += 1
    user_num += 1

  return lines


def main(num_users, num_groups, num_tables):

  lines = generate_hdfs_dump(num_tables, num_users, num_groups)
  # get the csv header from the keys of the first line
  header = lines[0].keys()

  # write lines as a csv file named hdfs_dump.csv
  with open("hdfs.csv", "w") as f:
    writer = csv.DictWriter(f, fieldnames=header)
    writer.writeheader()
    for line in lines:
      writer.writerow(line)


if __name__ == "__main__":
  if len(sys.argv) != 4:
    print(
        "Usage: python3 load_test_hdfs_dump_generator.py <num_users>"
        " <num_groups> <num_tables>"
    )
    sys.exit(1)
  main(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))

