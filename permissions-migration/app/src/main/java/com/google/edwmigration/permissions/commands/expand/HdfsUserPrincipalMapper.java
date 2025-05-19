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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.edwmigration.permissions.models.HdfsUser;
import com.google.edwmigration.permissions.models.Rule;

/*
 * Handles mapping from HDFS users to IAM principals.
 */
public class HdfsUserPrincipalMapper extends AbstractPrincipalMapper<HdfsUser> {

  private static final RuleSetCompiler RULE_SET_COMPILER =
      new RuleSetCompiler(/* variables= */ ImmutableList.of("user"));

  HdfsUserPrincipalMapper(
      ImmutableList<Rule> rules, StreamProcessor<HdfsUser> permissionStreamProcessor) {
    super("HDFS user", "user", RULE_SET_COMPILER, rules, permissionStreamProcessor);
  }

  @Override
  protected ImmutableMap<String, Object> toBaseObjectMap(HdfsUser user) {
    return ImmutableMap.of("action", "", "sources", ImmutableMultimap.of("hdfs/user", user.name()));
  }

  @Override
  protected ImmutableMap<String, Object> toDefaultObjectMap(HdfsUser user) {
    return ImmutableMap.of("type", "user");
  }
}
