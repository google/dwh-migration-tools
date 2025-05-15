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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import javax.annotation.Nullable;

@AutoValue
@JsonSerialize(as = PermissionsRuleset.class)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public abstract class PermissionsRuleset {

  public static final ObjectMapper YAML_MAPPER =
      new ObjectMapper(new YAMLFactory())
          .registerModule(new GuavaModule())
          .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

  @AutoValue
  @JsonSerialize(as = RoleMapping.class)
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public abstract static class RoleMapping {

    @JsonCreator
    public static RoleMapping create(
        @JsonProperty("rename_to") ImmutableList<String> renameTo,
        @JsonProperty("includes") ImmutableList<String> includes) {
      return builder().renameTo(renameTo).includes(includes).build();
    }

    @JsonProperty
    @Nullable
    public abstract ImmutableList<String> renameTo();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<String> includes();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder renameTo(List<String> value);

      public abstract Builder includes(List<String> value);

      public abstract RoleMapping build();
    }

    public static Builder builder() {
      return new AutoValue_PermissionsRuleset_RoleMapping.Builder();
    }
  }

  @AutoValue
  @JsonSerialize(as = GcsPermissionsRuleset.class)
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public abstract static class GcsPermissionsRuleset {

    @JsonCreator
    public static GcsPermissionsRuleset create(
        @JsonProperty("hdfs_rules") ImmutableList<Rule> hdfsMappingRules,
        @JsonProperty("ranger_hive_rules") ImmutableList<Rule> rangerHiveMappingRules,
        @JsonProperty("ranger_hdfs_rules") ImmutableList<Rule> rangerHdfsMappingRules) {
      return new AutoValue_PermissionsRuleset_GcsPermissionsRuleset(
          hdfsMappingRules, rangerHiveMappingRules, rangerHdfsMappingRules);
    }

    @JsonProperty
    @Nullable
    public abstract ImmutableList<Rule> hdfsMappingRules();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<Rule> rangerHiveMappingRules();

    @JsonProperty
    @Nullable
    // TODO why is this named hiveHdfs?
    public abstract ImmutableList<Rule> rangerHiveHdfsMappingRules();
  }

  @AutoValue
  @JsonSerialize(as = BqPermissionsRuleset.class)
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public abstract static class BqPermissionsRuleset {

    @JsonCreator
    public static BqPermissionsRuleset create(
        @JsonProperty("hdfs_rules") ImmutableList<Rule> hdfsMappingRules,
        @JsonProperty("ranger_hive_rules") ImmutableList<Rule> rangerHiveMappingRules,
        @JsonProperty("ranger_hdfs_rules") ImmutableList<Rule> rangerHdfsMappingRules) {
      return new AutoValue_PermissionsRuleset_BqPermissionsRuleset(
          hdfsMappingRules, rangerHiveMappingRules, rangerHdfsMappingRules);
    }

    @JsonProperty
    @Nullable
    public abstract ImmutableList<Rule> hdfsMappingRules();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<Rule> rangerHiveMappingRules();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<Rule> rangerHdfsMappingRules();
  }

  @JsonCreator
  public static PermissionsRuleset create(
      @JsonProperty("gcs") GcsPermissionsRuleset gcsPermissionsRuleset,
      @JsonProperty("bq") BqPermissionsRuleset bqPermissionsRuleset,
      @JsonProperty("roles") ImmutableMap<String, RoleMapping> roleMappings) {
    return new AutoValue_PermissionsRuleset(
        gcsPermissionsRuleset, bqPermissionsRuleset, roleMappings);
  }

  @JsonProperty
  @Nullable
  public abstract GcsPermissionsRuleset gcsPermissionsRuleset();

  @JsonProperty
  @Nullable
  public abstract BqPermissionsRuleset bqPermissionsRuleset();

  @JsonProperty
  @Nullable
  public abstract ImmutableMap<String, RoleMapping> roleMappings();
}
