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
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.permissions.commands.expand.CompiledRule.CompiledRuleMapField;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.models.Rule.RuleMapField;
import com.google.edwmigration.permissions.utils.RuleSetMapper.Action;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.CelTypes;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerBuilder;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleSetCompiler {

  @AutoValue
  public abstract static class EvalOutcome {

    static EvalOutcome create(Action action, ImmutableMap<String, Object> mappedObject) {
      return new AutoValue_RuleSetCompiler_EvalOutcome(action, mappedObject);
    }

    public abstract Action action();

    public abstract ImmutableMap<String, Object> mappedObject();
  }

  private static final Logger LOG = LoggerFactory.getLogger(RuleSetCompiler.class);

  private static final CelRuntime CEL_RUNTIME =
      CelRuntimeFactory.standardCelRuntimeBuilder().build();

  private final CelCompiler celPredicateCompiler;

  private final CelCompiler celExpressionCompiler;

  public RuleSetCompiler(List<String> variables) {
    celPredicateCompiler =
        addVariables(CelCompilerFactory.standardCelCompilerBuilder(), variables)
            .setProtoResultType(CelTypes.BOOL)
            .build();

    celExpressionCompiler =
        addVariables(CelCompilerFactory.standardCelCompilerBuilder(), variables).build();
  }

  public ImmutableList<CompiledRule> compile(List<Rule> rules) {
    return rules.stream()
        .map(
            rule -> {
              try {
                return compileRule(rule, celPredicateCompiler, celExpressionCompiler);
              } catch (CelValidationException | CelEvaluationException e) {
                throw new IllegalArgumentException(e);
              }
            })
        .collect(toImmutableList());
  }

  /**
   * Evaluates a list of rules against a given context.
   *
   * @param compiledRules List of rules to be evaluated. Rules are checked sequentially, the outcome
   *     of the first matching rule is returned. If no rule matches the context, a NO_MATCH result
   *     is returned.
   * @param context A map holding the CEL context rules are evaluated against.
   * @param baseObjectMapValues An object map that is copied to the output object, without having
   *     the possibility of CEL overriding or deleting these values.
   * @param defaultObjectMapValues An object map that is used to patch to the output object. Default
   *     values are copied to the output object only when the latter doesn't already have the given
   *     key.
   */
  public EvalOutcome eval(
      List<CompiledRule> compiledRules,
      ImmutableMap<String, Object> context,
      ImmutableMap<String, Object> baseObjectMapValues,
      ImmutableMap<String, Object> defaultObjectMapValues)
      throws CelEvaluationException {
    ImmutableMap<String, Object> objectMap =
        mergeMaps(
            baseObjectMapValues,
            defaultObjectMapValues,
            key ->
                new IllegalStateException(
                    String.format("Default key '%s' is reserved and cannot be overridden", key)));
    EvalOutcome evalOutcome = EvalOutcome.create(Action.NO_MATCH, objectMap);
    for (CompiledRule compiledRule : compiledRules) {
      boolean log = evalRuleLog(compiledRule, context);
      evalOutcome = evalRule(compiledRule, context, baseObjectMapValues, defaultObjectMapValues);
      if (evalOutcome.action() == Action.MAP || evalOutcome.action() == Action.SKIP) {
        // Stop processing and return the current outcome.
        if (log) {
          LOG.info(
              "Evaluated rule {} with context {}, result {}, output {}",
              compiledRule.source,
              context,
              evalOutcome.action(),
              evalOutcome.mappedObject());
        }
        return evalOutcome;
      } else if (log) {
        LOG.info("Evaluated rule {} with context {}, no match", compiledRule.source, context);
      }
    }
    return evalOutcome;
  }

  private CompiledRule compileRule(
      Rule sourceRule, CelCompiler whenCompiler, CelCompiler expressionCompiler)
      throws CelValidationException, CelEvaluationException {
    Optional<ImmutableMap<String, CompiledRuleMapField>> mapFields =
        Optional.ofNullable(sourceRule.mapFields())
            .map(
                sourceMapFields ->
                    sourceMapFields.entrySet().stream()
                        .collect(
                            collectingAndThen(
                                toMap(
                                    Entry::getKey,
                                    entry ->
                                        compileRuleMapField(expressionCompiler, entry.getValue())),
                                ImmutableMap::copyOf)));
    return new CompiledRule(
        sourceRule,
        compileExpression(whenCompiler, Optional.ofNullable(sourceRule.when()).orElse("true")),
        compileExpression(whenCompiler, Optional.ofNullable(sourceRule.log()).orElse("false")),
        mapFields,
        Optional.ofNullable(sourceRule.skip()));
  }

  /**
   * Evaluates the rule with a given context. The fields from baseObjectMapValues are returned in
   * the resulting object as-is, while defaultObjectMapValues are applied only when not overridden
   * by the evaluated map.
   */
  private static EvalOutcome evalRule(
      CompiledRule compiledRule,
      ImmutableMap<String, Object> context,
      ImmutableMap<String, Object> baseObjectMapValues,
      ImmutableMap<String, Object> defaultObjectMapValues)
      throws CelEvaluationException {
    if (compiledRule.when.eval(context) != Boolean.TRUE) {
      return EvalOutcome.create(
          Action.NO_MATCH,
          mergeMaps(
              baseObjectMapValues, defaultObjectMapValues, /* duplicateKeyToException= */ null));
    }
    if (compiledRule.skip.isPresent()) {
      return EvalOutcome.create(
          Action.SKIP,
          mergeMaps(
              baseObjectMapValues, defaultObjectMapValues, /* duplicateKeyToException= */ null));
    }
    ImmutableMap<String, Object> objectMap =
        mergeMaps(
            mergeMaps(
                // Apply base fields - these can't be overridden by the evaluation.
                baseObjectMapValues,
                // Patch the object with evaluation results.
                evalMap(compiledRule, context),
                key ->
                    new CelEvaluationException(
                        String.format(
                            "Output key '%s' is reserved and cannot be overridden", key))),
            // Apply defaults if needed.
            defaultObjectMapValues,
            /* duplicateKeyToException= */ null);
    return EvalOutcome.create(Action.MAP, objectMap);
  }

  /** Evaluates if a rule should be logged for a given context. */
  private static boolean evalRuleLog(
      CompiledRule compiledRule, ImmutableMap<String, Object> context)
      throws CelEvaluationException {
    return compiledRule.log.eval(context) == Boolean.TRUE;
  }

  /** Evaluates the map action fields. */
  private static ImmutableMap<String, Object> evalMap(
      CompiledRule compiledRule, ImmutableMap<String, Object> context) {
    return compiledRule.mapFields
        .orElseThrow(
            () -> new IllegalStateException("Invoked evalMap on a rule without map action"))
        .entrySet().stream()
        .collect(
            collectingAndThen(
                toMap(
                    Entry::getKey,
                    entry -> {
                      try {
                        return entry.getValue().eval(context);
                      } catch (CelEvaluationException e) {
                        throw new IllegalArgumentException(
                            "Failed to evaluate map field " + entry.getKey(), e);
                      }
                    }),
                ImmutableMap::copyOf));
  }

  private static CompiledRuleMapField compileRuleMapField(
      CelCompiler expressionCompiler, RuleMapField ruleMapField) {
    return new CompiledRuleMapField(
        ruleMapField,
        Optional.ofNullable(ruleMapField.expression())
            .map(
                expression -> {
                  try {
                    return compileExpression(expressionCompiler, expression);
                  } catch (CelValidationException | CelEvaluationException e) {
                    throw new IllegalArgumentException(
                        "Failed to compile expression " + expression, e);
                  }
                }),
        Optional.ofNullable(ruleMapField.value()));
  }

  private static CelRuntime.Program compileExpression(CelCompiler compiler, String expression)
      throws CelValidationException, CelEvaluationException {
    return CEL_RUNTIME.createProgram(compiler.compile(expression).getAst());
  }

  private static CelCompilerBuilder addVariables(
      CelCompilerBuilder builder, List<String> variables) {
    for (String variable : variables) {
      builder = builder.addVar(variable, CelTypes.DYN);
    }
    return builder;
  }

  /** Merge the given maps preserving values from the leftmost map on conflict. */
  private static <K, V, E extends Exception> ImmutableMap<K, V> mergeMaps(
      Map<K, V> base, Map<K, V> patch, Function<K, ? extends E> duplicateKeyToException) throws E {
    HashMap<K, V> result = new HashMap<>(base);
    for (Map.Entry<K, V> entry : patch.entrySet()) {
      if (result.containsKey(entry.getKey())) {
        if (duplicateKeyToException != null) {
          throw duplicateKeyToException.apply(entry.getKey());
        }
      } else {
        result.put(entry.getKey(), entry.getValue());
      }
    }
    return ImmutableMap.copyOf(result);
  }
}
