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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.permissions.commands.expand.RuleSetCompiler.EvalOutcome;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.models.Rule.RuleMapField;
import com.google.edwmigration.permissions.utils.RuleSetMapper.Action;
import dev.cel.runtime.CelEvaluationException;
import org.junit.jupiter.api.Test;

public class RuleSetCompilerTest {

  @Test
  public void eval_setsOutputWhenRuleMatches() throws CelEvaluationException {
    ImmutableList<Rule> rules =
        ImmutableList.of(
            Rule.builder()
                .when("true")
                .mapFields(
                    ImmutableMap.of(
                        "output1",
                        RuleMapField.create("object.value == '42' ? 'yes' : 'no'", /*value=*/ null),
                        "output2",
                        RuleMapField.create(/*expression= */ null, "value")))
                .build());
    RuleSetCompiler compiler = new RuleSetCompiler(ImmutableList.of("object"));
    ImmutableList<CompiledRule> compiledRules = compiler.compile(rules);

    EvalOutcome actual =
        compiler.eval(
            compiledRules,
            ImmutableMap.of("object", ImmutableMap.of("value", "42")),
            ImmutableMap.of("base", "base-value"),
            ImmutableMap.of("default", "default-value"));

    assertThat(actual.action()).isEqualTo(Action.MAP);
    assertThat(actual.mappedObject())
        .isEqualTo(
            ImmutableMap.of(
                "base", "base-value",
                "default", "default-value",
                "output1", "yes",
                "output2", "value"));
  }

  @Test
  public void eval_overridesDefaultObjectMap() throws CelEvaluationException {
    ImmutableList<Rule> rules =
        ImmutableList.of(
            Rule.builder()
                .when("true")
                .mapFields(
                    ImmutableMap.of(
                        "default", RuleMapField.create(/*expression= */ null, "new-value"),
                        "output1", RuleMapField.create(/*expression= */ null, "value")))
                .build());
    RuleSetCompiler compiler = new RuleSetCompiler(ImmutableList.of());
    ImmutableList<CompiledRule> compiledRules = compiler.compile(rules);

    EvalOutcome actual =
        compiler.eval(
            compiledRules,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of("default", "default-value"));

    assertThat(actual.action()).isEqualTo(Action.MAP);
    assertThat(actual.mappedObject())
        .isEqualTo(
            ImmutableMap.of(
                "default", "new-value",
                "output1", "value"));
  }

  @Test
  public void eval_throwsWhenOverridingBaseObjectMap() throws CelEvaluationException {
    ImmutableList<Rule> rules =
        ImmutableList.of(
            Rule.builder()
                .when("true")
                .mapFields(
                    ImmutableMap.of(
                        "base", RuleMapField.create(/*expression= */ null, "new-value"),
                        "output1", RuleMapField.create(/*expression= */ null, "value")))
                .build());
    RuleSetCompiler compiler = new RuleSetCompiler(ImmutableList.of("object"));
    ImmutableList<CompiledRule> compiledRules = compiler.compile(rules);

    CelEvaluationException actual =
        assertThrows(
            "CelEvaluationException was expected but not thrown",
            CelEvaluationException.class,
            () ->
                compiler.eval(
                    compiledRules,
                    /* context= */ ImmutableMap.of(),
                    /* baseObjectMapValues= */ ImmutableMap.of("base", "base-value"),
                    /* defaultObjectMapValues= */ ImmutableMap.of()));
    assertThat(actual.getMessage()).contains("base");
  }

  @Test
  public void eval_setsOutputFromFirstMatchingRule() throws CelEvaluationException {
    ImmutableList<Rule> rules =
        ImmutableList.of(
            Rule.builder()
                .when("false")
                .mapFields(
                    ImmutableMap.of(
                        "matched_rule", RuleMapField.create(/*expression= */ null, "rule1")))
                .build(),
            Rule.builder()
                .when("true")
                .mapFields(
                    ImmutableMap.of(
                        "matched_rule", RuleMapField.create(/*expression= */ null, "rule2")))
                .build(),
            Rule.builder()
                .when("true")
                .mapFields(
                    ImmutableMap.of(
                        "matched_rule", RuleMapField.create(/*expression= */ null, "rule3")))
                .build());
    RuleSetCompiler compiler = new RuleSetCompiler(ImmutableList.of("object"));
    ImmutableList<CompiledRule> compiledRules = compiler.compile(rules);

    EvalOutcome actual =
        compiler.eval(
            compiledRules,
            ImmutableMap.of("object", ImmutableMap.of()),
            ImmutableMap.of(),
            ImmutableMap.of());

    assertThat(actual.action()).isEqualTo(Action.MAP);
    assertThat(actual.mappedObject()).isEqualTo(ImmutableMap.of("matched_rule", "rule2"));
  }

  @Test
  public void eval_returnsNoMatchWithDefaultObjectWhenRulesDoNotMatch()
      throws CelEvaluationException {
    ImmutableList<Rule> rules =
        ImmutableList.of(
            Rule.builder()
                .when("false")
                .mapFields(
                    ImmutableMap.of(
                        "matched_rule", RuleMapField.create(/*expression= */ null, "rule1")))
                .build(),
            Rule.builder()
                .when("false")
                .mapFields(
                    ImmutableMap.of(
                        "matched_rule", RuleMapField.create(/*expression= */ null, "rule2")))
                .build());
    RuleSetCompiler compiler = new RuleSetCompiler(ImmutableList.of("object"));
    ImmutableList<CompiledRule> compiledRules = compiler.compile(rules);

    EvalOutcome actual =
        compiler.eval(
            compiledRules,
            ImmutableMap.of("object", ImmutableMap.of()),
            ImmutableMap.of("base", "base-value"),
            ImmutableMap.of("default", "default-value"));

    assertThat(actual.action()).isEqualTo(Action.NO_MATCH);
    assertThat(actual.mappedObject())
        .isEqualTo(
            ImmutableMap.of(
                "base", "base-value",
                "default", "default-value"));
  }

  @Test
  public void eval_returnsSkipWithDefaultObjectWhenSkipRuleMatches() throws CelEvaluationException {
    ImmutableList<Rule> rules =
        ImmutableList.of(
            Rule.builder()
                .when("false")
                .mapFields(
                    ImmutableMap.of(
                        "matched_rule", RuleMapField.create(/*expression= */ null, "rule1")))
                .build(),
            Rule.builder().when("true").skip(true).build());
    RuleSetCompiler compiler = new RuleSetCompiler(ImmutableList.of("object"));
    ImmutableList<CompiledRule> compiledRules = compiler.compile(rules);

    EvalOutcome actual =
        compiler.eval(
            compiledRules,
            ImmutableMap.of("object", ImmutableMap.of()),
            ImmutableMap.of("base", "base-value"),
            ImmutableMap.of("default", "default-value"));

    assertThat(actual.action()).isEqualTo(Action.SKIP);
    assertThat(actual.mappedObject())
        .isEqualTo(
            ImmutableMap.of(
                "base", "base-value",
                "default", "default-value"));
  }
}
