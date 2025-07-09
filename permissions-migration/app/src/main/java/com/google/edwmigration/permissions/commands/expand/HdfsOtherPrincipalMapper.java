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
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.utils.CollectionStreamProcessor;

/*
 * Handles mapping from HDFS other to an IAM principal.
 */
public class HdfsOtherPrincipalMapper extends AbstractPrincipalMapper<Boolean> {

  private static final RuleSetCompiler RULE_SET_COMPILER =
      new RuleSetCompiler(/* variables= */ ImmutableList.of("other"));

  HdfsOtherPrincipalMapper(ImmutableList<Rule> rules) {
    super(
        "HDFS other",
        "other",
        RULE_SET_COMPILER,
        rules,
        new CollectionStreamProcessor<>(ImmutableList.of(true)));
  }

  @Override
  protected ImmutableMap<String, Object> toBaseObjectMap(Boolean other) {
    return ImmutableMap.of("action", "", "sources", ImmutableMultimap.of("hdfs/other", "other"));
  }

  @Override
  protected ImmutableMap<String, Object> toDefaultObjectMap(Boolean other) {
    return ImmutableMap.of("type", "group");
  }
}
