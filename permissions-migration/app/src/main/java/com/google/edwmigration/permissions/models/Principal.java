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
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMultimap;
import com.google.edwmigration.permissions.utils.RuleSetMapper.Action;
import javax.annotation.Nullable;

@AutoValue
@JsonSerialize(as = Principal.class)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public abstract class Principal {

  @JsonCreator
  public static Principal create(
      @JsonProperty("action") Action action,
      @JsonProperty("email_address") String emailAddress,
      @JsonProperty("type") PrincipalType principalType,
      @JsonProperty("sources") ImmutableMultimap<String, String> sources) {
    return new AutoValue_Principal(action, emailAddress, principalType, sources);
  }

  @JsonProperty
  public abstract Action action();

  @JsonProperty
  @Nullable
  public abstract String emailAddress();

  @JsonProperty
  @Nullable
  public abstract PrincipalType type();

  @JsonProperty
  public abstract ImmutableMultimap<String, String> sources();
}
