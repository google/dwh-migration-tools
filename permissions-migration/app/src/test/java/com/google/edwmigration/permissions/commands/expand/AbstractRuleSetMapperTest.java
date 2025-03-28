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
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.models.Rule.RuleMapField;
import com.google.edwmigration.permissions.utils.AbstractRuleSetMapper;
import com.google.edwmigration.permissions.utils.CollectionStreamProcessor;
import com.google.edwmigration.permissions.utils.RuleSetMapper.Action;
import com.google.edwmigration.permissions.utils.RuleSetMapper.Result;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;

public class AbstractRuleSetMapperTest {

  @AutoValue
  @JsonSerialize(as = TestObject.class)
  public abstract static class TestObject {

    @JsonCreator
    public static TestObject create(
        @JsonProperty("string_field") String stringField,
        @JsonProperty("int_value") Integer intField) {
      return new AutoValue_AbstractRuleSetMapperTest_TestObject(stringField, intField);
    }

    @JsonProperty("string_field")
    @Nullable
    public abstract String stringField();

    @JsonProperty("int_field")
    @Nullable
    public abstract Integer intField();
  }

  private static class TestUserTestObjectMapper<T> extends AbstractRuleSetMapper<T, TestObject> {

    private final String variable;

    private final StreamProcessor<T> streamProcessor;

    TestUserTestObjectMapper(
        String variable, ImmutableList<Rule> rules, StreamProcessor<T> streamProcessor) {
      super(TestObject.class, "test user", new RuleSetCompiler(ImmutableList.of(variable)), rules);
      this.streamProcessor = streamProcessor;
      this.variable = variable;
    }

    @Override
    protected StreamProcessor<T> newStream() {
      return streamProcessor;
    }

    @Override
    protected ImmutableMap<String, Object> toContext(T source) {
      return ImmutableMap.of(variable, source);
    }
  }

  @Test
  public void run_supportObjectsAsContextVariable() {
    ImmutableList<Rule> rules =
        ImmutableList.of(
            createRule(
                /* when= */ "true",
                /* stringExpression= */ "object.string_field + '@google.com'",
                /* intExpression= */ "object.int_field"));
    ImmutableList<TestObject> objects = ImmutableList.of(TestObject.create("value", 42));
    TestUserTestObjectMapper<TestObject> expander =
        new TestUserTestObjectMapper<>("object", rules, new CollectionStreamProcessor<>(objects));

    ImmutableList<Result<TestObject>> result = expander.run();

    ImmutableList<Result<TestObject>> expected =
        ImmutableList.of(Result.create(Action.MAP, TestObject.create("value@google.com", 42)));
    assertThat(result).containsExactlyElementsIn(expected);
  }

  @Test
  public void run_supportPrimitivesWrappersOrStringsAsContextVariable() {
    ImmutableList<Rule> rules =
        ImmutableList.of(
            createRule(
                /* when= */ "true",
                /* stringExpression= */ "string(primitive * 2) + '@google.com'",
                /* intValue= */ 42));
    ImmutableList<Integer> primitives = ImmutableList.of(42);
    TestUserTestObjectMapper<Integer> expander =
        new TestUserTestObjectMapper<>(
            "primitive", rules, new CollectionStreamProcessor<>(primitives));

    ImmutableList<TestObject> principals = getResults(expander.run());

    ImmutableList<TestObject> expected = ImmutableList.of(TestObject.create("84@google.com", 42));
    assertThat(principals).containsExactlyElementsIn(expected);
  }

  @Test
  public void run_mapsObjectsAccordingToRuleset() {
    ImmutableList<Rule> rules =
        ImmutableList.of(
            createRule(
                /* when= */ "true",
                /* stringExpression= */ "user + '@google.com'",
                /* intValue= */ 42));
    ImmutableList<String> users = ImmutableList.of("user1", "user2", "user3");
    TestUserTestObjectMapper<String> expander =
        new TestUserTestObjectMapper<>("user", rules, new CollectionStreamProcessor<>(users));

    ImmutableList<Result<TestObject>> principals = expander.run();

    ImmutableList<Result<TestObject>> expected =
        ImmutableList.of(
            Result.create(Action.MAP, TestObject.create("user1@google.com", 42)),
            Result.create(Action.MAP, TestObject.create("user2@google.com", 42)),
            Result.create(Action.MAP, TestObject.create("user3@google.com", 42)));
    assertThat(principals).containsExactlyElementsIn(expected);
  }

  @Test
  public void run_throwsWhenNoRuleMatches() {
    ImmutableList<Rule> rules = ImmutableList.of();
    ImmutableList<String> users = ImmutableList.of("user1", "user2", "user3");
    TestUserTestObjectMapper<String> expander =
        new TestUserTestObjectMapper<>("user", rules, new CollectionStreamProcessor<>(users));

    assertThrows(
        "IllegalArgumentException was expected but not thrown",
        IllegalArgumentException.class,
        expander::run);
  }

  @Test
  public void run_appliesTheFirstRuleThatMatches() {
    ImmutableList<Rule> rules =
        ImmutableList.of(
            createRule(
                /* when= */ "false",
                /* stringExpression= */ "user + '@no-match.example.com'",
                /* intExpression= */ "42"),
            createRule(
                /* when= */ "true",
                /* stringExpression= */ "user + '@google.com'",
                /* intExpression= */ "42"),
            createRule(
                /* when= */ "true",
                /* stringExpression= */ "user + '@example.com'",
                /* intExpression= */ "42"));
    ImmutableList<String> users = ImmutableList.of("user1", "user2", "user3");
    TestUserTestObjectMapper<String> expander =
        new TestUserTestObjectMapper<>("user", rules, new CollectionStreamProcessor<>(users));

    ImmutableList<Result<TestObject>> principals = expander.run();

    ImmutableList<Result<TestObject>> expected =
        ImmutableList.of(
            Result.create(Action.MAP, TestObject.create("user1@google.com", 42)),
            Result.create(Action.MAP, TestObject.create("user2@google.com", 42)),
            Result.create(Action.MAP, TestObject.create("user3@google.com", 42)));
    assertThat(principals).containsExactlyElementsIn(expected);
  }

  @Test
  public void run_appliesSkipWhenRuleMatches() {
    ImmutableList<Rule> rules =
        ImmutableList.of(
            Rule.builder().when("user == 'user1'").skip(true).build(),
            createRule(
                /* when= */ "true",
                /* stringExpression= */ "user + '@google.com'",
                /* intExpression= */ "42"));
    ImmutableList<String> users = ImmutableList.of("user1", "user2");
    TestUserTestObjectMapper<String> expander =
        new TestUserTestObjectMapper<>("user", rules, new CollectionStreamProcessor<>(users));

    ImmutableList<Result<TestObject>> principals = expander.run();

    ImmutableList<Result<TestObject>> expected =
        ImmutableList.of(
            Result.create(Action.SKIP, TestObject.create(null, null)),
            Result.create(Action.MAP, TestObject.create("user2@google.com", 42)));
    assertThat(principals).containsExactlyElementsIn(expected);
  }

  private static Rule createRule(String whenExpression, String stringExpression, int intValue) {
    return Rule.builder()
        .when(whenExpression)
        .mapFields(
            ImmutableMap.of(
                "string_field", RuleMapField.create(stringExpression, /*value=*/ null),
                "int_field", RuleMapField.create(/* expression= */ null, String.valueOf(intValue))))
        .build();
  }

  private static Rule createRule(
      String whenExpression, String stringExpression, String intExpression) {
    return Rule.builder()
        .when(whenExpression)
        .mapFields(
            ImmutableMap.of(
                "string_field", RuleMapField.create(stringExpression, /*value=*/ null),
                "int_field", RuleMapField.create(intExpression, /* value= */ null)))
        .build();
  }

  private <T> ImmutableList<T> getResults(ImmutableList<Result<T>> results) {
    return results.stream().map(Result::value).collect(toImmutableList());
  }
}
