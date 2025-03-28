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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

@AutoValue
@JsonSerialize(as = Permissions.class)
public abstract class Permissions {

  public static final ObjectMapper YAML_MAPPER =
      new ObjectMapper(new YAMLFactory())
          .registerModule(new GuavaModule())
          .setSerializationInclusion(JsonInclude.Include.NON_NULL);

  @JsonCreator
  public static Permissions create(
      @JsonProperty("permissions") ImmutableList<IamBinding> gcs_iamBindings) {
    return new AutoValue_Permissions(gcs_iamBindings);
  }

  @JsonProperty
  public abstract ImmutableList<IamBinding> permissions();
}
