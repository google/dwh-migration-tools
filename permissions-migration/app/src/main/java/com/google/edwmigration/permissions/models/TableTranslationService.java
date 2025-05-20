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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.List;

@AutoValue
@JsonSerialize(as = TableTranslationService.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class TableTranslationService {
  public static final ObjectMapper YAML_MAPPER =
      new ObjectMapper(new YAMLFactory()).registerModule(new GuavaModule());

  @JsonCreator
  public static TableTranslationService create(
      @JsonProperty("sourceName") String sourceName,
      @JsonProperty("targetName") String targetName,
      @JsonProperty("sourceLocations") List<String> sourceLocations,
      @JsonProperty("targetLocations") List<String> targetLocations) {
    return new AutoValue_TableTranslationService(
        sourceName,
        targetName,
        ImmutableList.copyOf(sourceLocations),
        ImmutableList.copyOf(targetLocations));
  }

  @JsonProperty
  public abstract String sourceName();

  @JsonProperty
  public abstract String targetName();

  @JsonProperty
  public abstract List<String> sourceLocations();

  @JsonProperty
  public abstract List<String> targetLocations();
}
