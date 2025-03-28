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
import static com.google.edwmigration.permissions.commands.buildcommand.AbstractRangerPermissionMapper.RANGER_DEFAULT_PRIORITY;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.edwmigration.permissions.commands.buildcommand.AbstractRangerPermissionMapper.RangerPrincipal;
import com.google.edwmigration.permissions.commands.buildcommand.AbstractRangerPermissionMapper.RangerType;
import com.google.edwmigration.permissions.models.IamBinding;
import com.google.edwmigration.permissions.models.IamBinding.ResourceType;
import com.google.edwmigration.permissions.models.Principal;
import com.google.edwmigration.permissions.models.PrincipalType;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.models.Table;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy.PolicyItem;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy.PolicyItemAccess;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy.PolicyResource;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Service;
import com.google.edwmigration.permissions.utils.CollectionStreamProcessor;
import com.google.edwmigration.permissions.utils.ObjectToMapConverter;
import com.google.edwmigration.permissions.utils.RuleSetMapper;
import com.google.edwmigration.permissions.utils.RuleSetMapper.Action;
import org.junit.jupiter.api.Test;

public class RangerHdfsToGcsIamBindingMapperTest {

  private static final ImmutableList<Rule> MATCH_ALL_RULE_SET =
      ImmutableList.of(Rule.builder().when("true").mapFields(ImmutableMap.of()).build());

  @Test
  public void run_mapsHdfsGroupToPrincipal() {
    ObjectToMapConverter mapConverter = new ObjectToMapConverter(RangerDumpFormat.MAPPER);
    ImmutableList<Table> tables =
        ImmutableList.of(
            Table.create(
                "table1",
                "schema",
                "hdfs://cluster-m/schema/table1",
                "gs://test/schema/table1",
                null));
    ImmutableList<Principal> principals =
        ImmutableList.of(
            Principal.create(
                Action.MAP,
                "user1@google.com",
                PrincipalType.USER,
                ImmutableMultimap.of("ranger/user", "user1")));
    ImmutableList<Policy> policies =
        ImmutableList.of(
            Policy.builder()
                .id(1)
                .name("policy1")
                .service("hdfs")
                .resources(
                    ImmutableMap.of(
                        "path",
                        PolicyResource.create(ImmutableList.of("/schema/table*"), false, true)))
                .policyItems(
                    ImmutableList.of(
                        PolicyItem.builder()
                            .accesses(ImmutableList.of(PolicyItemAccess.create("read", true)))
                            .users(ImmutableList.of("user1"))
                            .build()))
                .build());
    ImmutableList<Service> services =
        ImmutableList.of(Service.builder().id(1L).name("hdfs").type("hdfs").build());

    RangerHdfsToGcsIamBindingMapper mapper =
        new RangerHdfsToGcsIamBindingMapper(
            MATCH_ALL_RULE_SET,
            new CollectionStreamProcessor<>(tables),
            new CollectionStreamProcessor<>(principals),
            new CollectionStreamProcessor<>(policies),
            new CollectionStreamProcessor<>(services));
    ImmutableList<IamBinding> actual =
        mapper.run().stream().map(RuleSetMapper.Result::value).collect(toImmutableList());

    ImmutableList<IamBinding> expected =
        ImmutableList.of(
            IamBinding.builder()
                .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                .resourcePath("gs://test/schema/table1")
                .principal("user:user1@google.com")
                .role("roles/storage.objectViewer")
                .priority(RANGER_DEFAULT_PRIORITY)
                .sourcePrincipals(
                    ImmutableMultimap.of(
                        "ranger",
                        mapConverter.convertToMap(
                            RangerPrincipal.create(RangerType.USER, "user1"))))
                .sourcePermissions(
                    ImmutableMultimap.of("ranger", mapConverter.convertToMap(policies.get(0))))
                .build());
    assertThat(actual).containsExactlyElementsIn(expected);
  }
}
