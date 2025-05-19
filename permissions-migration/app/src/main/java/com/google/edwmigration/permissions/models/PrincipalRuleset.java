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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import javax.annotation.Nullable;

@AutoValue
@JsonSerialize(as = PrincipalRuleset.class)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public abstract class PrincipalRuleset {

  public static final ObjectMapper YAML_MAPPER =
      new ObjectMapper(new YAMLFactory()).registerModule(new GuavaModule());

  @AutoValue
  @JsonSerialize(as = HdfsPrincipalRuleset.class)
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public abstract static class HdfsPrincipalRuleset {
    @JsonCreator
    static HdfsPrincipalRuleset create(
        @JsonProperty("user_rules") ImmutableList<Rule> userRules,
        @JsonProperty("group_rules") ImmutableList<Rule> groupRules,
        @JsonProperty("other_rules") ImmutableList<Rule> otherRules) {
      return new AutoValue_PrincipalRuleset_HdfsPrincipalRuleset(userRules, groupRules, otherRules);
    }

    @Nullable
    public abstract ImmutableList<Rule> userRules();

    @Nullable
    public abstract ImmutableList<Rule> groupRules();

    @Nullable
    public abstract ImmutableList<Rule> otherRules();
  }

  @AutoValue
  @JsonSerialize(as = RangerPrincipalRuleset.class)
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public abstract static class RangerPrincipalRuleset {
    @JsonCreator
    static RangerPrincipalRuleset create(
        @JsonProperty("user_rules") ImmutableList<Rule> userRules,
        @JsonProperty("group_rules") ImmutableList<Rule> groupRules,
        @JsonProperty("role_rules") ImmutableList<Rule> roleRules) {
      return new AutoValue_PrincipalRuleset_RangerPrincipalRuleset(
          userRules, groupRules, roleRules);
    }

    @JsonProperty
    @Nullable
    public abstract ImmutableList<Rule> userRules();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<Rule> groupRules();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<Rule> roleRules();
  }

  @JsonCreator
  static PrincipalRuleset create(
      @JsonProperty("hdfs") HdfsPrincipalRuleset hdfsPrincipalRuleset,
      @JsonProperty("ranger") RangerPrincipalRuleset rangerPrincipalRuleset) {
    return new AutoValue_PrincipalRuleset(hdfsPrincipalRuleset, rangerPrincipalRuleset);
  }

  @JsonProperty
  @Nullable
  public abstract HdfsPrincipalRuleset hdfsPrincipalRuleset();

  @JsonProperty
  @Nullable
  public abstract RangerPrincipalRuleset rangerPrincipalRuleset();
}
