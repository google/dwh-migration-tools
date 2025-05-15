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
package com.google.edwmigration.permissions.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;

@AutoValue
@JsonSerialize(as = Rule.class)
public abstract class Rule {

  @AutoValue
  @JsonSerialize(as = RuleMapField.class)
  public abstract static class RuleMapField {

    @JsonCreator
    public static RuleMapField create(
        @JsonProperty("expression") String expression, @JsonProperty("value") String value) {
      if ((expression == null) == (value == null)) {
        throw new IllegalArgumentException("Either expression or value must be set");
      }
      return new AutoValue_Rule_RuleMapField(expression, value);
    }

    @JsonProperty
    @Nullable
    public abstract String expression();

    @JsonProperty
    @Nullable
    public abstract String value();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder when(String value);

    public abstract Builder mapFields(Map<String, RuleMapField> value);

    public abstract Builder skip(Boolean value);

    public abstract Builder log(String value);

    public abstract Rule build();
  }

  public static Builder builder() {
    return new AutoValue_Rule.Builder();
  }

  @JsonCreator
  public static Rule create(
      @JsonProperty("when") String when,
      @JsonProperty("map") Map<String, RuleMapField> mapFields,
      @JsonProperty("skip") Boolean skip,
      @JsonProperty("log") String log) {
    boolean skipIsSet = skip != null;
    boolean mapIsSet = mapFields != null;
    if (skipIsSet == mapIsSet) {
      throw new IllegalArgumentException("Either map or skip must be specified");
    }
    return builder().when(when).mapFields(mapFields).skip(skip).log(log).build();
  }

  @JsonProperty
  @Nullable
  public abstract String when();

  @JsonProperty("map")
  @Nullable
  public abstract ImmutableMap<String, RuleMapField> mapFields();

  @JsonProperty
  @Nullable
  public abstract Boolean skip();

  @JsonProperty
  @Nullable
  public abstract String log();
}
