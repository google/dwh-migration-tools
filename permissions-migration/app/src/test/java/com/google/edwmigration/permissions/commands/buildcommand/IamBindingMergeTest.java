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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.permissions.models.IamBinding;
import com.google.edwmigration.permissions.models.IamBinding.ResourceType;
import com.google.edwmigration.permissions.models.PermissionsRuleset.RoleMapping;
import com.google.edwmigration.permissions.utils.RuleSetMapper;
import org.junit.jupiter.api.Test;

public class IamBindingMergeTest {

  public static class FixtureMapper implements RuleSetMapper<IamBinding> {

    private final ImmutableList<IamBinding> iamBindings;

    public FixtureMapper(IamBinding... iamBindings) {
      this.iamBindings = ImmutableList.copyOf(iamBindings);
    }

    @Override
    public ImmutableList<RuleSetMapper.Result<IamBinding>> run() {
      return iamBindings.stream()
          .map(iamBinding -> RuleSetMapper.Result.create(Action.MAP, iamBinding))
          .collect(toImmutableList());
    }
  }

  @Test
  public void run_removesDuplicates() {
    PermissionMerge merge =
        PermissionMerge.newInstance(
            ImmutableList.of(
                new FixtureMapper(
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table1")
                        .principal("user1@google.com")
                        .role("writer")
                        .priority(0)
                        .build(),
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table1")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(0)
                        .build(),
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table2")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(0)
                        .build()),
                new FixtureMapper(
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table1")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(0)
                        .build(),
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table3")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(0)
                        .build(),
                    IamBinding.builder()
                        .resourceType(ResourceType.BQ_TABLE)
                        .resourcePath("/table1")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(0)
                        .build())),
            /* roles= */ null);

    ImmutableList<IamBinding> actual = merge.run();

    assertThat(actual)
        .containsExactlyElementsIn(
            ImmutableList.of(
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table1")
                    .principal("user1@google.com")
                    .role("writer")
                    .priority(0)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table1")
                    .principal("user1@google.com")
                    .role("reader")
                    .priority(0)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table2")
                    .principal("user1@google.com")
                    .role("reader")
                    .priority(0)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table3")
                    .principal("user1@google.com")
                    .role("reader")
                    .priority(0)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.BQ_TABLE)
                    .resourcePath("/table1")
                    .principal("user1@google.com")
                    .role("reader")
                    .priority(0)
                    .build()));
  }

  @Test
  public void run_picksRolesWithHighestPriority() {
    PermissionMerge merge =
        PermissionMerge.newInstance(
            ImmutableList.of(
                new FixtureMapper(
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table1")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(0)
                        .build(),
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table1")
                        .principal("user1@google.com")
                        .role("writer")
                        .priority(0)
                        .build(),
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table2")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(0)
                        .build()),
                new FixtureMapper(
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table1")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(10)
                        .build(),
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table3")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(10)
                        .build())),
            /* roles= */ null);

    ImmutableList<IamBinding> actual = merge.run();

    assertThat(actual)
        .containsExactlyElementsIn(
            ImmutableList.of(
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table1")
                    .principal("user1@google.com")
                    .role("reader")
                    .priority(10)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table2")
                    .principal("user1@google.com")
                    .role("reader")
                    .priority(0)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table3")
                    .principal("user1@google.com")
                    .role("reader")
                    .priority(10)
                    .build()));
  }

  @Test
  public void run_removesIncludedRoles() {
    PermissionMerge merge =
        PermissionMerge.newInstance(
            ImmutableList.of(
                new FixtureMapper(
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table1")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(0)
                        .build(),
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table2")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(0)
                        .build()),
                new FixtureMapper(
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table1")
                        .principal("user1@google.com")
                        .role("writer")
                        .priority(0)
                        .build(),
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table3")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(0)
                        .build())),
            /* roles= */ ImmutableMap.of(
                "writer", RoleMapping.builder().includes(ImmutableList.of("reader")).build()));

    ImmutableList<IamBinding> actual = merge.run();

    assertThat(actual)
        .containsExactlyElementsIn(
            ImmutableList.of(
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table1")
                    .principal("user1@google.com")
                    .role("writer")
                    .priority(0)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table2")
                    .principal("user1@google.com")
                    .role("reader")
                    .priority(0)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table3")
                    .principal("user1@google.com")
                    .role("reader")
                    .priority(0)
                    .build()));
  }

  @Test
  public void run_transitivelyRemovesIncludedRoles() {
    PermissionMerge merge =
        PermissionMerge.newInstance(
            ImmutableList.of(
                new FixtureMapper(
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table1")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(0)
                        .build()),
                new FixtureMapper(
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table1")
                        .principal("user1@google.com")
                        .role("admin")
                        .priority(0)
                        .build())),
            /* roles= */ ImmutableMap.of(
                "writer", RoleMapping.builder().includes(ImmutableList.of("reader")).build(),
                "admin", RoleMapping.builder().includes(ImmutableList.of("writer")).build()));

    ImmutableList<IamBinding> actual = merge.run();

    assertThat(actual)
        .containsExactlyElementsIn(
            ImmutableList.of(
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table1")
                    .principal("user1@google.com")
                    .role("admin")
                    .priority(0)
                    .build()));
  }

  @Test
  public void run_renameRoles() {
    PermissionMerge merge =
        PermissionMerge.newInstance(
            ImmutableList.of(
                new FixtureMapper(
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table1")
                        .principal("user1@google.com")
                        .role("execute")
                        .priority(0)
                        .build(),
                    IamBinding.builder()
                        .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                        .resourcePath("/table2")
                        .principal("user1@google.com")
                        .role("reader")
                        .priority(0)
                        .build())),
            /* roles= */ ImmutableMap.of(
                "execute",
                RoleMapping.builder().renameTo(ImmutableList.of("reader", "writer")).build()));

    ImmutableList<IamBinding> actual = merge.run();

    assertThat(actual)
        .containsExactlyElementsIn(
            ImmutableList.of(
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table1")
                    .principal("user1@google.com")
                    .role("reader")
                    .priority(0)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table1")
                    .principal("user1@google.com")
                    .role("writer")
                    .priority(0)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("/table2")
                    .principal("user1@google.com")
                    .role("reader")
                    .priority(0)
                    .build()));
  }
}
