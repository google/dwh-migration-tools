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
package com.google.edwmigration.permissions.commands.expand;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.edwmigration.permissions.models.HdfsGroup;
import com.google.edwmigration.permissions.models.Principal;
import com.google.edwmigration.permissions.models.PrincipalType;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.models.Rule.RuleMapField;
import com.google.edwmigration.permissions.utils.CollectionStreamProcessor;
import com.google.edwmigration.permissions.utils.RuleSetMapper;
import com.google.edwmigration.permissions.utils.RuleSetMapper.Action;
import org.junit.jupiter.api.Test;

public class HdfsGroupPrincipalMapperTest {

  private static final ImmutableList<Rule> MATCH_ALL_RULE_SET =
      ImmutableList.of(
          Rule.builder()
              .when("true")
              .mapFields(
                  ImmutableMap.of(
                      "email_address",
                      RuleMapField.create("group.name + '@google.com'", /*value=*/ null)))
              .build());

  @Test
  public void run_mapsHdfsGroupToPrincipal() {
    ImmutableList<HdfsGroup> users = ImmutableList.of(HdfsGroup.create("group1"));
    HdfsGroupPrincipalMapper expander =
        new HdfsGroupPrincipalMapper(MATCH_ALL_RULE_SET, new CollectionStreamProcessor<>(users));

    ImmutableList<Principal> principals =
        expander.run().stream().map(RuleSetMapper.Result::value).collect(toImmutableList());

    ImmutableList<Principal> expected =
        ImmutableList.of(
            Principal.create(
                Action.MAP,
                "group1@google.com",
                PrincipalType.GROUP,
                ImmutableMultimap.of("hdfs/group", "group1")));
    assertThat(principals).containsExactlyElementsIn(expected);
  }
}
