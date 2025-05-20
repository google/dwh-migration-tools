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

import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.models.Rule.RuleMapField;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import java.util.Optional;

public class CompiledRule {

  public static class CompiledRuleMapField {

    public final RuleMapField source;

    public final Optional<CelRuntime.Program> expression;

    public final Optional<String> value;

    public CompiledRuleMapField(
        RuleMapField source, Optional<CelRuntime.Program> expression, Optional<String> value) {
      this.source = source;
      this.expression = expression;
      this.value = value;
    }

    /** Evaluate the field value for the given context. */
    public Object eval(ImmutableMap<String, Object> context) throws CelEvaluationException {
      if (expression.isPresent()) {
        return expression.get().eval(context);
      }
      return value.get();
    }
  }

  public final Rule source;

  public final CelRuntime.Program when;

  public final CelRuntime.Program log;

  public final Optional<ImmutableMap<String, CompiledRuleMapField>> mapFields;

  public final Optional<Boolean> skip;

  public CompiledRule(
      Rule source,
      CelRuntime.Program when,
      CelRuntime.Program log,
      Optional<ImmutableMap<String, CompiledRuleMapField>> mapFields,
      Optional<Boolean> skip) {
    this.source = source;
    this.when = when;
    this.log = log;
    this.mapFields = mapFields;
    this.skip = skip;
  }
}
