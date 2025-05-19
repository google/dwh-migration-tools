/*
 * Copyright 2022-2025 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.edwmigration.permissions.commands.buildcommand;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.Identity;
import com.google.cloud.Role;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.permissions.ExtraPermissions;
import com.google.edwmigration.permissions.FakeIamClient;
import com.google.edwmigration.permissions.commands.apply.PermissionsApplier;
import com.google.edwmigration.permissions.models.IamBinding;
import com.google.edwmigration.permissions.models.IamBinding.ResourceType;
import com.google.edwmigration.permissions.models.Permissions;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class PermissionsApplierTest {

  @Test
  public void apply_createsManagedFolders() throws IOException {
    FakeIamClient gcsManagedFolderClient = new FakeIamClient();
    PermissionsApplier applier =
        new PermissionsApplier(
            ImmutableMap.of(ResourceType.GCS_MANAGED_FOLDER, gcsManagedFolderClient));

    Permissions permissions =
        Permissions.create(
            ImmutableList.of(
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("gs://cluster-1-bucket/table-a")
                    .principal("user:user1@example.com")
                    .role("roles/storage.objectUser")
                    .priority(10)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("gs://cluster-1-bucket/table-a")
                    .principal("user:user2@example.com")
                    .role("roles/storage.objectViewer")
                    .priority(10)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("gs://cluster-2-bucket/table-a")
                    .principal("user:user1@example.com")
                    .role("roles/storage.objectUser")
                    .priority(10)
                    .build()));
    applier.apply(permissions, ExtraPermissions.KEEP);

    assertThat(gcsManagedFolderClient.checkPathExists("gs://cluster-1-bucket/table-a")).isTrue();
    assertThat(gcsManagedFolderClient.checkPathExists("gs://cluster-2-bucket/table-a")).isTrue();
  }

  @Test
  public void apply_addsNewPermissions() throws IOException {
    FakeIamClient gcsManagedFolderClient = new FakeIamClient();
    PermissionsApplier applier =
        new PermissionsApplier(
            ImmutableMap.of(ResourceType.GCS_MANAGED_FOLDER, gcsManagedFolderClient));

    Permissions permissions =
        Permissions.create(
            ImmutableList.of(
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("gs://cluster-1-bucket/table-a")
                    .principal("user:user1@example.com")
                    .role("roles/storage.objectUser")
                    .priority(10)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("gs://cluster-1-bucket/table-a")
                    .principal("user:user2@example.com")
                    .role("roles/storage.objectViewer")
                    .priority(10)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("gs://cluster-2-bucket/table-a")
                    .principal("user:user1@example.com")
                    .role("roles/storage.objectUser")
                    .priority(10)
                    .build()));
    applier.apply(permissions, ExtraPermissions.KEEP);

    assertThat(
            gcsManagedFolderClient.checkPermissionExists(
                "gs://cluster-1-bucket/table-a",
                Role.of("roles/storage.objectUser"),
                Identity.valueOf("user:user1@example.com")))
        .isTrue();
    assertThat(
            gcsManagedFolderClient.checkPermissionExists(
                "gs://cluster-1-bucket/table-a",
                Role.of("roles/storage.objectViewer"),
                Identity.valueOf("user:user2@example.com")))
        .isTrue();
    assertThat(
            gcsManagedFolderClient.checkPermissionExists(
                "gs://cluster-2-bucket/table-a",
                Role.of("roles/storage.objectUser"),
                Identity.valueOf("user:user1@example.com")))
        .isTrue();
  }

  @Test
  public void apply_keepsExisingPermissions() throws IOException {
    FakeIamClient fakeIamClient = new FakeIamClient();
    PermissionsApplier applier =
        new PermissionsApplier(ImmutableMap.of(ResourceType.GCS_MANAGED_FOLDER, fakeIamClient));
    fakeIamClient.addIdentityToIamPolicy(
        "gs://cluster-1-bucket/table-a",
        Role.of("roles/storage.objectUser"),
        Identity.valueOf("user:old-user@example.com"));

    Permissions permissions =
        Permissions.create(
            ImmutableList.of(
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("gs://cluster-1-bucket/table-a")
                    .principal("user:user1@example.com")
                    .role("roles/storage.objectUser")
                    .priority(10)
                    .build()));
    applier.apply(permissions, ExtraPermissions.KEEP);

    assertThat(
            fakeIamClient.checkPermissionExists(
                "gs://cluster-1-bucket/table-a",
                Role.of("roles/storage.objectUser"),
                Identity.valueOf("user:user1@example.com")))
        .isTrue();
    assertThat(
            fakeIamClient.checkPermissionExists(
                "gs://cluster-1-bucket/table-a",
                Role.of("roles/storage.objectUser"),
                Identity.valueOf("user:old-user@example.com")))
        .isTrue();
  }

  @Test
  public void apply_purgeExisingPermissions() throws IOException {
    FakeIamClient gcsManagedFolderClient = new FakeIamClient();
    PermissionsApplier applier =
        new PermissionsApplier(
            ImmutableMap.of(ResourceType.GCS_MANAGED_FOLDER, gcsManagedFolderClient));
    gcsManagedFolderClient.addIdentityToIamPolicy(
        "gs://cluster-1-bucket/table-a",
        Role.of("roles/storage.objectUser"),
        Identity.valueOf("user:old-user@example.com"));

    Permissions permissions =
        Permissions.create(
            ImmutableList.of(
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("gs://cluster-1-bucket/table-a")
                    .principal("user:user1@example.com")
                    .role("roles/storage.objectUser")
                    .priority(10)
                    .build()));
    applier.apply(permissions, ExtraPermissions.PURGE);

    assertThat(
            gcsManagedFolderClient.checkPermissionExists(
                "gs://cluster-1-bucket/table-a",
                Role.of("roles/storage.objectUser"),
                Identity.valueOf("user:old-user@example.com")))
        .isFalse();
  }
}
