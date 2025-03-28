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
import unittest

from tf_generator.utils import aggregate_permissions


class TestPermissionAggregation(unittest.TestCase):
    def test_no_permissions(self):
        # Given
        # Permission config with no permission
        yaml_permission_config = []

        # When
        # Function to aggregate permissions has been called
        managed_folder_role_to_principles = aggregate_permissions(yaml_permission_config)

        # Then
        # Empty dict should be returned
        self.assertTrue(len(managed_folder_role_to_principles) == 0)

    def test_one_permission(self):
        # Given
        # Permission config with one permission
        yaml_permission_config = [{
            "resourceType": "GCS_MANAGED_FOLDER",
            "resourcePath": "gs://bqms-migration-demo-data/crm/leads",
            "principal": "serviceAccount:sales-sa@bqms-migration-demo.iam.gserviceaccount.com",
            "role": "roles/storage.objectUser",
        }]

        # When
        # Function to aggregate permissions has been called
        managed_folder_role_to_principles = aggregate_permissions(yaml_permission_config)

        # Then
        # The dict with list of principals (service accounts)
        # to the managed folder role should be returned
        self.assertDictEqual(
            managed_folder_role_to_principles,
            {"bqms-migration-demo-data": {"crm/leads": {"roles/storage.objectUser": [
                "serviceAccount:sales-sa@bqms-migration-demo.iam.gserviceaccount.com"
            ]}}}
        )

    def test_same_folder_role_to_different_principles(self):
        # Given
        # Permission config with several permissions
        yaml_permission_config = [
            {
                "resourceType": "GCS_MANAGED_FOLDER",
                "resourcePath": "gs://bqms-migration-demo-data/crm/leads",
                "principal": "serviceAccount:sales-sa@bqms-migration-demo.iam.gserviceaccount.com",
                "role": "roles/storage.objectUser",
            }, {
                "resourceType": "GCS_MANAGED_FOLDER",
                "resourcePath": "gs://bqms-migration-demo-data/crm/leads",
                "principal": "serviceAccount:admins-sa@bqms-migration-demo.iam.gserviceaccount.com",
                "role": "roles/storage.objectUser",
            }
        ]

        # When
        # Function to aggregate permissions has been called
        managed_folder_role_to_principles = aggregate_permissions(yaml_permission_config)

        # Then
        # The dict with the same key (folder role)
        # to the list of principals (service accounts) should be returned
        self.assertDictEqual(
            managed_folder_role_to_principles,
            {"bqms-migration-demo-data": {"crm/leads": {"roles/storage.objectUser": [
                "serviceAccount:sales-sa@bqms-migration-demo.iam.gserviceaccount.com",
                "serviceAccount:admins-sa@bqms-migration-demo.iam.gserviceaccount.com",
            ]}}}
        )

    def test_different_folder_roles_to_the_same_principle(self):
        # Given
        # Permission config with several permissions
        yaml_permission_config = [
            {
                "resourceType": "GCS_MANAGED_FOLDER",
                "resourcePath": "gs://bqms-migration-demo-data/crm/leads",
                "principal": "serviceAccount:sales-sa@bqms-migration-demo.iam.gserviceaccount.com",
                "role": "roles/storage.objectUser",
            }, {
                "resourceType": "GCS_MANAGED_FOLDER",
                "resourcePath": "gs://bqms-migration-demo-data/crm/admins",
                "principal": "serviceAccount:admins-sa@bqms-migration-demo.iam.gserviceaccount.com",
                "role": "roles/storage.objectUser",
            }
        ]

        # When
        # Function to aggregate permissions has been called
        managed_folder_role_to_principles = aggregate_permissions(yaml_permission_config)

        # Then
        # The dict with different keys (folder roles)
        # to principals (service accounts) should be returned
        self.assertDictEqual(
            managed_folder_role_to_principles,
             {"bqms-migration-demo-data": {
                "crm/leads": {"roles/storage.objectUser": [
                    "serviceAccount:sales-sa@bqms-migration-demo.iam.gserviceaccount.com",
                ]},
                "crm/admins": {"roles/storage.objectUser": [
                    "serviceAccount:admins-sa@bqms-migration-demo.iam.gserviceaccount.com",
                ]},
            }}
        )

    def test_different_buckets_to_the_same_principal(self):
        # Given
        # Permission config with several permissions
        yaml_permission_config = [
            {
                "resourceType": "GCS_MANAGED_FOLDER",
                "resourcePath": "gs://bqms-migration-demo-data1/crm/leads",
                "principal": "serviceAccount:sales-sa@bqms-migration-demo.iam.gserviceaccount.com",
                "role": "roles/storage.objectUser",
            }, {
                "resourceType": "GCS_MANAGED_FOLDER",
                "resourcePath": "gs://bqms-migration-demo-data2/crm/leads",
                "principal": "serviceAccount:sales-sa@bqms-migration-demo.iam.gserviceaccount.com",
                "role": "roles/storage.objectUser",
            }
        ]

        # When
        # Function to aggregate permissions has been called
        managed_folder_role_to_principles = aggregate_permissions(yaml_permission_config)

        # Then
        # The dict with different keys (bucket > folder roles)
        # to principals (service accounts) should be returned
        self.assertDictEqual(
            managed_folder_role_to_principles,
            {
                "bqms-migration-demo-data1": {
                    "crm/leads": {"roles/storage.objectUser": [
                        "serviceAccount:sales-sa@bqms-migration-demo.iam.gserviceaccount.com",
                    ]}
                },
                "bqms-migration-demo-data2": {
                   "crm/leads": {"roles/storage.objectUser": [
                        "serviceAccount:sales-sa@bqms-migration-demo.iam.gserviceaccount.com",
                    ]}
                }
            }
        )
