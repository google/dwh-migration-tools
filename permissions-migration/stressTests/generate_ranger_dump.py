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
import datetime as dt
import json
import random
import sys
import uuid


def random_datetime(start_date, end_date):
  """Return a random datetime between start_date and end_date."""
  start_datetime = dt.datetime.strptime(start_date, "%d-%m-%Y")
  end_datetime = dt.datetime.strptime(end_date, "%d-%m-%Y")

  # Calculate the time difference between the dates
  time_between_dates = end_datetime - start_datetime
  days_between_dates = time_between_dates.days

  # Generate a random number of days within the range
  random_number_of_days = random.randrange(days_between_dates)

  # Add the random number of days to the start date
  random_date = start_datetime + dt.timedelta(days=random_number_of_days)

  return random_date


def random_timestamp(start_date, end_date):
  """return a random timestamp between start_date and end_date in seconds."""
  return random_datetime(start_date, end_date).timestamp()


def random_datetime_string(start_date, end_date):
  """return a random datetime string between start_date and end_date formatted as ISO

  Args:
    start_date: the start date in the format "dd-mm-yyyy"
    end_date: the end date in the format "dd-mm-yyyy"

  Returns:
  """
  return (
      random_datetime(start_date, end_date)
      .replace(tzinfo=dt.timezone.utc)
      .strftime("%Y-%m-%dT%H:%M:%S%z")
      .replace("+0000", "Z")
  )


def create_groups(num_groups):
  # create a list of group objects each containing the following fields:
  # - createDate: a timestamp in ISO format, randomly generated between
  # 2024-01-01 and 2024-08-01
  # - description: a string containing a description, sequentially increasing in
  # the format "Description{index}"
  # - id: sequenally increasing int
  # - isVisible: always set to 1
  # - name: a string containing the group name in the format "group{index}"
  # - owner: always set to "rangerusersync"
  # - updateDate: same as createDate
  # - updatedBy: always set to "rangerusersync"
  # - groupSource: always set to 1
  # - groupType: always set to 1
  groups = []
  for i in range(num_groups):
    timestamp = random_datetime_string("01-01-2024", "01-08-2024")
    groups.append({
        "createDate": timestamp,
        "description": "Description" + str(i),
        "id": i,
        "isVisible": 1,
        "name": "group" + str(i),
        "owner": "rangerusersync",
        "updateDate": timestamp,
        "updatedBy": "rangerusersync",
        "groupSource": 1,
        "groupType": 1,
    })
  return groups


def create_users(num_users, groups):
  # create a list of user objects each containing the following fields:
  # - createDate: a timestamp in ISO format, randomly generated between
  # 2024-01-01 and 2024-08-01
  # - firstName: a string containing a first name, sequentially increasing in
  # the format "FirstName{index}"
  # - lastName: a string containing a last name, sequentially increasing in the
  # format "LastName{index}"
  # - id: sequenally increasing int
  # - name: a string containing the user name in the format "user{index}"
  # - isVisible: always set to 1
  # - owner: always set to "Admin"
  # - password: always set to "*****"
  # - status: always set to 1
  # - updatedBy: always set to "Admin"
  # - updateDate: same as createDate
  # - userRoleList: a list of strings always containing "ROLE_USER"
  # - userSource: always set to 1
  # - groupIdList: a list of integers with ids of 5-10 randomly selected groups
  # from the group list "groups"
  # - groupNameList: a list of strings with names of the groups in groupIdList
  users = []
  for i in range(num_users):
    timestamp = random_datetime_string("01-01-2024", "01-08-2024")
    # randomly choose between 5 and 10 ids from the groups list
    groups_ids = random.sample(
        range(0, len(groups)),
        random.randint(max(0, min(10, len(groups)) - 5), min(11, len(groups))),
    )
    users.append({
        "createDate": timestamp,
        "firstName": "FirstName" + str(i),
        "lastName": "LastName" + str(i),
        "id": i,
        "name": "user" + str(i),
        "isVisible": 1,
        "owner": "Admin",
        "password": "*****",
        "status": 1,
        "updatedBy": "Admin",
        "updateDate": timestamp,
        "userRoleList": ["ROLE_USER"],
        "userSource": 1,
        "groupIdList": groups_ids,
        "groupNameList": [groups[i]["name"] for i in groups_ids],
    })
  return users


def create_roles(num_roles, users, groups):
  # create a list of role objects each containing the following fields:
  # - createTime: unix timestamp in seconds, randomly generated between
  # 2024-01-01 and 2024-08-01
  # - createdBy: always set to "Admin"
  # - description: a string containing a description, sequentially increasing in
  # the format "Description{index}"
  # - id: sequenally increasing int
  # - isEnabled: always set to true
  # - name: a string containing the role name in the format "role{index}"
  # - updateTime: same as createTime
  # - updatedBy: always set to "Admin"
  # - groups: a list of objects created from randomly selected 5 - 10 groups by using the following fields:
  #   - name: the name fields of the selected group
  #   - isAdmin: always set to false
  # - users: a list of objects created from randomly selected 5 - 10 users by using the following fields:
  #   - name: the name field of the selected user
  #   - isAdmin: always set to to false
  # - roles: an empty list
  # - options: an empty object
  roles = []
  for i in range(num_roles):
    timestamp = random_timestamp("01-01-2024", "01-08-2024")
    # randomly choose between 5 and 10 ids from the groups list
    groups_ids = random.sample(
        range(0, len(groups)),
        random.randint(max(0, min(10, len(groups)) - 5), min(11, len(groups))),
    )
    # randomly choose between 5 and 10 ids from the users list
    users_ids = random.sample(
        range(0, len(users)),
        random.randint(max(0, min(10, len(users)) - 5), min(11, len(users))),
    )
    roles.append({
        "createTime": timestamp,
        "createdBy": "Admin",
        "description": "Description" + str(i),
        "id": i,
        "isEnabled": True,
        "name": "role" + str(i),
        "updateTime": timestamp,
        "updatedBy": "Admin",
        "groups": [
            {
                "name": groups[i]["name"],
                "isAdmin": False,
            }
            for i in groups_ids
        ],
        "users": [
            {
                "name": users[i]["name"],
                "isAdmin": False,
            }
            for i in users_ids
        ],
        "roles": [],
        "options": {},
    })
  return roles


# generate an object based on the following json example:
# {"allowExceptions":[],"dataMaskPolicyItems":[],"denyExceptions":[],"denyPolicyItems":[],"description":"","guid":"31aef7bf-6df2-4bc7-ab49-447b55058165","id":8,"isAuditEnabled":true,"isDenyAllElse":false,"isEnabled":true,"name":"user1 db1 tab1 all cols access","options":{},"policyItems":[{"accesses":[{"isAllowed":true,"type":"select"},{"isAllowed":true,"type":"update"},{"isAllowed":true,"type":"create"},{"isAllowed":true,"type":"alter"},{"isAllowed":true,"type":"read"},{"isAllowed":true,"type":"write"}],"conditions":[],"delegateAdmin":false,"groups":[],"roles":[],"users":["user1"]}],"policyLabels":[],"policyPriority":0,"policyType":0,"resources":{"column":{"isExcludes":false,"isRecursive":false,"values":["*"]},"database":{"isExcludes":false,"isRecursive":false,"values":["db1"]},"table":{"isExcludes":false,"isRecursive":false,"values":["tab1"]}},"rowFilterPolicyItems":[],"service":"hive-dataproc","serviceType":"3","validitySchedules":[],"version":1,"zoneName":""}
# the guid should be a uuid
# the id should be a number specified as a parameter
# the name should be a string specified as a parameter
# the groups, roles and users should be lists of strings specified as parameters
def create_ranger_hive_policy(
    policy_id, name, groups, roles, users, table_name
):
  return {
      "allowExceptions": [],
      "dataMaskPolicyItems": [],
      "denyExceptions": [],
      "denyPolicyItems": [],
      "description": "",
      "guid": uuid.uuid4().hex,
      "id": policy_id,
      "isAuditEnabled": True,
      "isDenyAllElse": False,
      "isEnabled": True,
      "name": name,
      "options": {},
      "policyItems": [{
          "accesses": [
              {"isAllowed": True, "type": "select"},
              {"isAllowed": True, "type": "update"},
              {"isAllowed": True, "type": "create"},
              {"isAllowed": True, "type": "alter"},
              {"isAllowed": True, "type": "read"},
              {"isAllowed": True, "type": "write"},
          ],
          "conditions": [],
          "delegateAdmin": False,
          "groups": groups,
          "roles": roles,
          "users": users,
      }],
      "policyLabels": [],
      "policyPriority": 0,
      "policyType": 0,
      "resources": {
          "column": {
              "isExcludes": False,
              "isRecursive": False,
              "values": ["*"],
          },
          "database": {
              "isExcludes": False,
              "isRecursive": False,
              "values": ["db0"],
          },
          "table": {
              "isExcludes": False,
              "isRecursive": False,
              "values": [table_name],
          },
      },
      "rowFilterPolicyItems": [],
      "service": "hive-dataproc",
      "serviceType": "3",
      "validitySchedules": [],
      "version": 1,
      "zoneName": "",
  }


# generate a list of policies using the create_ranger_hive_policy function
# take the number of groups, roles and users as parameters and create separate policies for each group, role and user
# the policy_ids should grow from 1 to num_groups + num_roles + num_users
# the users should be generated as a list of single string in the format of "user" + n where n is 0 - num_users - 1
# the roles should be generated as a list of single string in the format of "role" + n where n is 0 - num_roles - 1
# the groups should be generated as a list of single string in the format of "group" + n where n is 0 - num_groups - 1
# the name of the policy should be policy + str(policy_id)
def create_ranger_hive_policies(num_groups, num_roles, num_users, num_tables):
  policies = []
  group_number = 0
  user_number = 0
  role_number = 0
  for i in range(num_tables):
    if i % 3 == 0:
      policies.append(
          create_ranger_hive_policy(
              i,
              "policy" + str(i),
              ["group" + str(group_number % num_groups)],
              [],
              [],
              "tab" + str(i),
              )
      )
      group_number += 1
    elif i % 3 == 1:
      policies.append(
          create_ranger_hive_policy(
              i,
              "policy" + str(i),
              [],
              ["role" + str(role_number % num_roles)],
              [],
              "tab" + str(i),
              )
      )
      role_number += 1
    else:
      policies.append(
          create_ranger_hive_policy(
              i,
              "policy" + str(i),
              [],
              [],
              ["user" + str(user_number % num_users)],
              "tab" + str(i),
              )
      )
      user_number += 1

  return policies


# generate an object based on the following json example:
# {"guid":"ccbf304f-b00e-4bd7-a15f-14f448ffd07a","id":14,"isAuditEnabled":true,"isDenyAllElse":false,"isEnabled":true,"name":"all databases access","policyItems":[{"accesses":[{"isAllowed":true,"type":"read"},{"isAllowed":true,"type":"write"},{"isAllowed":true,"type":"execute"}],"delegateAdmin":false,"groups":["accounting","sales","marketing","dba"]},{"accesses":[{"isAllowed":true,"type":"read"},{"isAllowed":true,"type":"execute"}],"delegateAdmin":false,"users":["replication"]}],"policyPriority":0,"policyType":0,"resources":{"path":{"isExcludes":false,"isRecursive":true,"values":["/user/hive/warehouse"]}},"service":"hadoop-dataproc","serviceType":"hdfs","version":10}
# the guid should be a uuid
# the id should be a number specified as a parameter policy_id
# the groups, roles and users should be lists of strings specified as parameters
# the table_name contains the name of the table to be used in the resources section
def generate_ranger_hdfs_policy(policy_id, groups, roles, users, table_name):
  return {
      "guid": uuid.uuid4().hex,
      "id": policy_id,
      "isAuditEnabled": True,
      "isDenyAllElse": False,
      "isEnabled": True,
      "name": "policy" + str(policy_id),
      "policyItems": [
          {
              "accesses": [
                  {"isAllowed": True, "type": "read"},
                  {"isAllowed": True, "type": "write"},
                  {"isAllowed": True, "type": "execute"},
              ],
              "delegateAdmin": False,
              "groups": groups,
              "roles": roles,
              "users": users,
          },
      ],
      "policyPriority": 0,
      "policyType": 0,
      "resources": {
          "path": {
              "isExcludes": False,
              "isRecursive": True,
              "values": ["/db1/" + table_name],
          },
      },
      "service": "hadoop-dataproc",
      "serviceType": "hdfs",
      "version": 10,
  }


# generate a list of policies using the create_ranger_hdfs_policy function
# take the number of groups, roles and users as parameters and create separate policies for each group, role and user
# the number of tables is a parameter and table names should be generated as "tab" + policy_id % num_tables
# the policy_ids should grow from 1 to num_groups + num_roles + num_users
# the users should be generated as a list of single string in the format of "user" + n where n is 0 - num_users - 1
# the roles should be generated as a list of single string in the format of "role" + n where n is 0 - num_roles - 1
# the groups should be generated as a list of single string in the format of "group" + n where n is 0 - num_groups - 1
# the name of the policy should be policy + str(policy_id)
def create_ranger_hdfs_policies(
    num_groups, num_roles, num_users, num_tables, policy_name_offset
):
  policies = []
  group_number = 0
  user_number = 0
  role_number = 0
  for i in range(num_tables):
    if i % 3 == 0:
      policies.append(
          generate_ranger_hdfs_policy(
              i + policy_name_offset,
              ["group" + str(group_number % num_groups)],
              [],
              [],
              "tab" + str(i),
              )
      )
      group_number += 1
    elif i % 3 == 1:
      policies.append(
          generate_ranger_hdfs_policy(
              i + policy_name_offset,
              [],
              ["role" + str(role_number % num_roles)],
              [],
              "tab" + str(i),
              )
      )
      role_number += 1
    else:
      policies.append(
          generate_ranger_hdfs_policy(
              i + policy_name_offset,
              [],
              [],
              ["user" + str(user_number % num_users)],
              "tab" + str(i),
              )
      )
      user_number += 1

  return policies


def main(num_groups, num_users, num_roles, num_tables):

  groups = create_groups(num_groups)
  users = create_users(num_users, groups)
  roles = create_roles(num_roles, users, groups)

  # save the groups in a jsonl file named groups.jsonl
  with open("groups.jsonl", "w") as f:
    for group in groups:
      f.write(json.dumps(group) + "\n")
  # save the users in a jsonl file named users.jsonl
  with open("users.jsonl", "w") as f:
    for user in users:
      f.write(json.dumps(user) + "\n")
  # save the roles in a jsonl file named roles.jsonl
  with open("roles.jsonl", "w") as f:
    for role in roles:
      f.write(json.dumps(role) + "\n")

  policies = create_ranger_hive_policies(
      num_groups, num_roles, num_users, num_tables
  )
  policies += create_ranger_hdfs_policies(
      num_groups,
      num_roles,
      num_users,
      num_tables,
      num_groups + num_roles + num_users,
      )
  # save the policies in a jsonl file named policies.jsonl
  with open("policies.jsonl", "w") as f:
    for policy in policies:
      f.write(json.dumps(policy) + "\n")


if __name__ == "__main__":
  if len(sys.argv) != 5:
    print(
        "Usage: python3 load_test_ranger_dump_generator.py <num_groups>"
        " <num_users> <num_roles> <num_tables>"
    )
    sys.exit(1)
  main(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]))
