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
import javax.annotation.Nullable;

@AutoValue
@JsonSerialize(as = IamBinding.class)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public abstract class IamBinding {

  public enum ResourceType {
    GCS_MANAGED_FOLDER,
    BQ_TABLE
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder resourceType(ResourceType value);

    public abstract Builder resourcePath(String value);

    public abstract Builder principal(String value);

    public abstract Builder role(String value);

    public abstract Builder priority(int value);

    public abstract Builder sourcePermissions(ImmutableMultimap<String, Object> value);

    public abstract Builder sourcePrincipals(ImmutableMultimap<String, Object> value);

    public abstract IamBinding build();
  }

  public static Builder builder() {
    return new AutoValue_IamBinding.Builder();
  }

  public abstract Builder toBuilder();

  @JsonCreator
  public static IamBinding create(
      @JsonProperty("resourceType") ResourceType resourceType,
      @JsonProperty("resourcePath") String resourcePath,
      @JsonProperty("principal") String principal,
      @JsonProperty("role") String role,
      @JsonProperty("priority") int priority,
      @JsonProperty("source_permissions") ImmutableMultimap<String, Object> sourcePermissions,
      @JsonProperty("source_principals") ImmutableMultimap<String, Object> sourcePrincipals) {
    return builder()
        .resourceType(resourceType)
        .resourcePath(resourcePath)
        .principal(principal)
        .role(role)
        .priority(priority)
        .sourcePermissions(sourcePermissions)
        .sourcePrincipals(sourcePrincipals)
        .build();
  }

  @JsonProperty
  @Nullable
  public abstract ResourceType resourceType();

  @JsonProperty
  @Nullable
  public abstract String resourcePath();

  /**
   * An IAM principal identifier, see https://cloud.google.com/iam/docs/principal-identifiers#v1 for
   * the format.
   */
  @JsonProperty
  public abstract String principal();

  @JsonProperty
  public abstract String role();

  @JsonProperty
  public abstract int priority();

  @JsonProperty
  @Nullable
  public abstract ImmutableMultimap<String, Object> sourcePermissions();

  @JsonProperty
  @Nullable
  public abstract ImmutableMultimap<String, Object> sourcePrincipals();
}
