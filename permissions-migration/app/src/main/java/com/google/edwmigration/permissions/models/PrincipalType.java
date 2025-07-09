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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.Field;

// All principal identifier types are listed here:
// https://cloud.google.com/iam/docs/principal-identifiers
// I only added a subset of it. The enum can be expanded in the future.
public enum PrincipalType {
  @JsonProperty("user")
  USER,

  @JsonProperty("serviceAccount")
  SERVICE_ACCOUNT,

  @JsonProperty("group")
  GROUP,

  @JsonProperty("domain")
  DOMAIN;

  public String jsonValue() {
    try {
      Field enumField = PrincipalType.class.getField(name());
      return enumField.getAnnotation(JsonProperty.class).value();
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException(
          "Failed to retrieve @JsonProperty for PrincipalType " + name());
    }
  }
}
