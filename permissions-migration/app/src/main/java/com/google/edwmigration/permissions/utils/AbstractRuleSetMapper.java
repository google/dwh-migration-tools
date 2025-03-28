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
package com.google.edwmigration.permissions.utils;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import com.google.edwmigration.permissions.ProcessingException;
import com.google.edwmigration.permissions.commands.expand.CompiledRule;
import com.google.edwmigration.permissions.commands.expand.RuleSetCompiler;
import com.google.edwmigration.permissions.commands.expand.RuleSetCompiler.EvalOutcome;
import com.google.edwmigration.permissions.commands.expand.StreamProcessor;
import com.google.edwmigration.permissions.models.Rule;
import com.google.errorprone.annotations.ForOverride;
import dev.cel.runtime.CelEvaluationException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRuleSetMapper<T, R> implements RuleSetMapper<R> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractRuleSetMapper.class);

  private static final ObjectToMapConverter MAP_CONVERTER =
      new ObjectToMapConverter(
          new ObjectMapper()
              .registerModule(new GuavaModule())
              .registerModule(new JavaTimeModule()));

  private final Class<? extends R> resultClass;

  private final List<CompiledRule> compiledRules;

  private final String sourceName;

  private final RuleSetCompiler ruleSetCompiler;

  protected AbstractRuleSetMapper(
      Class<? extends R> resultClass,
      String sourceName,
      RuleSetCompiler ruleSetCompiler,
      ImmutableList<Rule> rules) {
    this.resultClass = resultClass;
    this.sourceName = sourceName;
    this.ruleSetCompiler = ruleSetCompiler;
    this.compiledRules = ImmutableList.copyOf(ruleSetCompiler.compile(rules));
  }

  @ForOverride
  protected abstract StreamProcessor<T> newStream();

  public ImmutableList<RuleSetMapper.Result<R>> run() {
    return newStream().process(this::map);
  }

  /** Retrieve the CEL context for a given source. */
  protected abstract ImmutableMap<String, Object> toContext(T source);

  protected ImmutableMap<String, Object> toBaseObjectMap(T source) {
    return ImmutableMap.of();
  }

  protected ImmutableMap<String, Object> toDefaultObjectMap(T source) {
    return ImmutableMap.of();
  }

  protected ImmutableMap<String, Object> toOutputObjectMap(
      T source, ImmutableMap<String, Object> mappedObject, Action action) {
    return mappedObject;
  }

  private ImmutableList<RuleSetMapper.Result<R>> map(Stream<T> sourceStream) {
    ImmutableList<RuleSetMapper.Result<R>> result =
        sourceStream
            .map(
                source -> {
                  try {
                    return map(source);
                  } catch (CelEvaluationException e) {
                    throw new ProcessingException(
                        String.format("Failed to map %s item: %s", sourceName, source), e);
                  }
                })
            .collect(toImmutableList());
    LOG.info("Mapped {} {} items", result.size(), sourceName);
    return result;
  }

  private RuleSetMapper.Result<R> map(T source) throws CelEvaluationException {
    // TODO(aleofreddi): explore how to port this to proto.
    EvalOutcome evalOutcome =
        ruleSetCompiler.eval(
            compiledRules,
            /* context= */ toContext(source).entrySet().stream()
                .collect(
                    toImmutableMap(
                        Map.Entry::getKey, entry -> convertContextValue(entry.getValue()))),
            /* baseObjectMapValues= */ toBaseObjectMap(source),
            /* defaultObjectMapValues= */ toDefaultObjectMap(source));
    HashMap<String, Object> mappedObject = new HashMap<>(evalOutcome.mappedObject());
    if (evalOutcome.action() == Action.NO_MATCH) {
      throw new IllegalArgumentException(
          String.format("No rule matches %s item: %s", sourceName, source));
    }
    return RuleSetMapper.Result.create(
        evalOutcome.action(),
        MAP_CONVERTER.convertFromMap(
            toOutputObjectMap(source, ImmutableMap.copyOf(mappedObject), evalOutcome.action()),
            resultClass));
  }

  private Object convertContextValue(Object source) {
    // Do not serialize primitive types and Strings.
    if (source instanceof String || Primitives.isWrapperType(source.getClass())) {
      return source;
    }
    if (source instanceof Collection) {
      return ((Collection<?>) source).stream().map(MAP_CONVERTER::convertToMap);
    }
    return MAP_CONVERTER.convertToMap(source);
  }
}
