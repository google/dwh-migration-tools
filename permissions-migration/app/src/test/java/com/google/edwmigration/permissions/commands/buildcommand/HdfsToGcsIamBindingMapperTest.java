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
import static com.google.edwmigration.permissions.utils.RuleSetMapper.Action;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.edwmigration.permissions.commands.buildcommand.AbstractHdfsToIamBindingMapper.HdfsPrincipal;
import com.google.edwmigration.permissions.commands.buildcommand.AbstractHdfsToIamBindingMapper.HdfsPrincipalType;
import com.google.edwmigration.permissions.models.*;
import com.google.edwmigration.permissions.models.IamBinding.ResourceType;
import com.google.edwmigration.permissions.utils.CollectionStreamProcessor;
import com.google.edwmigration.permissions.utils.ObjectToMapConverter;
import com.google.edwmigration.permissions.utils.RuleSetMapper;
import org.junit.jupiter.api.Test;

public class HdfsToGcsIamBindingMapperTest {

  private static final ImmutableList<Rule> MATCH_ALL_RULE_SET =
      ImmutableList.of(Rule.builder().when("true").mapFields(ImmutableMap.of()).build());

  @Test
  public void run_mapsHdfsGroupToPrincipal() {
    ObjectToMapConverter mapConverter = new ObjectToMapConverter(HdfsPermission.CSV_MAPPER);
    ImmutableList<Table> tables =
        ImmutableList.of(
            Table.create(
                "table1",
                "schema",
                "hdfs://cluster-m/schema/table1",
                "gs://test/schema/table1",
                null),
            Table.create(
                "table2",
                "schema",
                "hdfs://cluster-m/schema/table2",
                "gs://test/schema/table2",
                null));
    ImmutableList<Principal> principals =
        ImmutableList.of(
            Principal.create(
                Action.MAP,
                "user1@google.com",
                PrincipalType.USER,
                ImmutableMultimap.of("hdfs/user", "user1")),
            Principal.create(
                Action.MAP,
                "user1@google.com",
                PrincipalType.USER,
                ImmutableMultimap.of("hdfs/user", "user2")),
            Principal.create(Action.SKIP, null, null, ImmutableMultimap.of("hdfs/user", "user3")),
            Principal.create(
                Action.MAP,
                "team@google.com",
                PrincipalType.GROUP,
                ImmutableMultimap.of("hdfs/group", "group1")),
            Principal.create(Action.SKIP, null, null, ImmutableMultimap.of("hdfs/group", "group2")),
            Principal.create(
                Action.MAP,
                "all@google.com",
                PrincipalType.GROUP,
                ImmutableMultimap.of("hdfs/other", "other")));
    ImmutableList<HdfsPermission> permissions =
        ImmutableList.of(
            HdfsPermission.create(
                /* path= */ "/schema/table1",
                /* fileType= */ "D",
                /* fileSize= */ 0L,
                /* owner= */ "user1",
                /* group= */ "group1",
                /* permission= */ "rwxr-x---",
                /* modificatonTime= */ null,
                /* fileCount= */ null,
                /* dirCount= */ null,
                /* storagePolicy= */ null),
            HdfsPermission.create(
                /* path= */ "/schema/table2",
                /* fileType= */ "D",
                /* fileSize= */ 0L,
                /* owner= */ "user3",
                /* group= */ "group2",
                /* permission= */ "rwxr-xr-x",
                /* modificatonTime= */ null,
                /* fileCount= */ null,
                /* dirCount= */ null,
                /* storagePolicy= */ null));

    HdfsToGcsIamBindingMapper mapper =
        new HdfsToGcsIamBindingMapper(
            MATCH_ALL_RULE_SET,
            new CollectionStreamProcessor<>(tables),
            new CollectionStreamProcessor<>(principals),
            new CollectionStreamProcessor<>(permissions));
    ImmutableList<IamBinding> actual =
        mapper.run().stream().map(RuleSetMapper.Result::value).collect(toImmutableList());

    ImmutableList<IamBinding> expected =
        ImmutableList.of(
            IamBinding.builder()
                .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                .resourcePath("gs://test/schema/table1")
                .principal("user:user1@google.com")
                .role("roles/storage.objectUser")
                .priority(0)
                .sourcePrincipals(
                    ImmutableMultimap.of(
                        "hdfs",
                        mapConverter.convertToMap(
                            HdfsPrincipal.create(HdfsPrincipalType.USER, "user1"))))
                .sourcePermissions(
                    ImmutableMultimap.of("hdfs", mapConverter.convertToMap(permissions.get(0))))
                .build(),
            IamBinding.builder()
                .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                .resourcePath("gs://test/schema/table1")
                .principal("group:team@google.com")
                .role("roles/storage.objectViewer")
                .priority(0)
                .sourcePrincipals(
                    ImmutableMultimap.of(
                        "hdfs",
                        mapConverter.convertToMap(
                            HdfsPrincipal.create(HdfsPrincipalType.GROUP, "group1"))))
                .sourcePermissions(
                    ImmutableMultimap.of("hdfs", mapConverter.convertToMap(permissions.get(0))))
                .build(),
            IamBinding.builder()
                .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                .resourcePath("gs://test/schema/table2")
                .principal("group:all@google.com")
                .role("roles/storage.objectViewer")
                .priority(0)
                .sourcePrincipals(
                    ImmutableMultimap.of(
                        "hdfs",
                        mapConverter.convertToMap(
                            HdfsPrincipal.create(HdfsPrincipalType.OTHER, "other"))))
                .sourcePermissions(
                    ImmutableMultimap.of("hdfs", mapConverter.convertToMap(permissions.get(1))))
                .build());
    assertThat(actual).containsExactlyElementsIn(expected);
  }
}
