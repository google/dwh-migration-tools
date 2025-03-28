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
import com.google.edwmigration.permissions.models.Principal;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.utils.AbstractRuleSetMapper;
import java.util.HashMap;
import java.util.Map;

public class AbstractPrincipalMapper<T> extends AbstractRuleSetMapper<T, Principal> {

  private final String sourceVariable;

  private final StreamProcessor<T> streamProcessor;

  @Override
  protected StreamProcessor<T> newStream() {
    return streamProcessor;
  }

  /** Principal mappers have a single variable in the context. */
  @Override
  protected ImmutableMap<String, Object> toContext(T source) {
    return ImmutableMap.of(sourceVariable, source);
  }

  /** Populate the action field on mapped Principals. */
  @Override
  protected ImmutableMap<String, Object> toOutputObjectMap(
      T source, ImmutableMap<String, Object> mappedObject, Action action) {
    Map<String, Object> mutableMap = new HashMap<>(mappedObject);
    mutableMap.put("action", action.toString());
    return ImmutableMap.copyOf(mutableMap);
  }

  AbstractPrincipalMapper(
      String sourceName,
      String sourceVariable,
      RuleSetCompiler ruleSetCompiler,
      ImmutableList<Rule> rules,
      StreamProcessor<T> streamProcessor) {
    super(Principal.class, sourceName, ruleSetCompiler, rules);
    this.streamProcessor = streamProcessor;
    this.sourceVariable = sourceVariable;
  }
}
