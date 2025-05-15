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

import static com.google.common.truth.Truth.assertThat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ObjectToMapConverterTest {

  @AutoValue
  @JsonSerialize(as = TestObject.class)
  public abstract static class TestObject {

    @JsonCreator
    public static TestObject create(
        @JsonProperty("string_field") String stringField, @JsonProperty("int_value") int intField) {
      return new AutoValue_ObjectToMapConverterTest_TestObject(stringField, intField);
    }

    @JsonProperty("string_field")
    public abstract String stringField();

    @JsonProperty("int_field")
    public abstract int intField();
  }

  @Test
  public void convertToMap_convertsObjectToMap() {
    ObjectToMapConverter converter = new ObjectToMapConverter(new ObjectMapper());
    TestObject testObject = TestObject.create("value", 42);

    Object actual = converter.convertToMap(testObject);

    assertThat(actual).isInstanceOf(Map.class);
    assertThat((Map<?, ?>) actual)
        .containsExactlyEntriesIn(ImmutableMap.of("string_field", "value", "int_field", 42));
  }

  @Test
  public void convertFromMap_convertsMapToObject() {
    ObjectToMapConverter converter = new ObjectToMapConverter(new ObjectMapper());
    ImmutableMap<String, Object> testObject =
        ImmutableMap.of("string_field", "value", "int_field", 42);

    TestObject actual = converter.convertFromMap(testObject, TestObject.class);

    assertThat(actual).isEqualTo(TestObject.create("value", 42));
  }
}
