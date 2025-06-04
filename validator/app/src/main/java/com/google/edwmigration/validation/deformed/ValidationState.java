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
package com.google.edwmigration.validation.deformed;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.HashMap;
import java.util.Map;

/** Entire object-level validation state */
public class ValidationState {
  public final Map<String, FieldValidationState> validationState = new HashMap<>();

  public void addError(String fieldName, String errorMessage) {
    FieldValidationState fieldState =
        this.validationState.computeIfAbsent(fieldName, k -> new FieldValidationState());
    fieldState.errors.add(errorMessage);
    fieldState.isValid = false;
  }

  public boolean hasErrors() {
    return this.validationState.values().stream().anyMatch(f -> f == null || !f.isValid);
  }

  public Map<String, FieldValidationState> getAll() {
    return this.validationState;
  }

  public void merge(String prefix, ValidationState other) {
    for (Map.Entry<String, FieldValidationState> entry : other.getAll().entrySet()) {
      String mergedKey = prefix + "." + entry.getKey();
      validationState.put(mergedKey, entry.getValue());
    }
  }

  /**
   * Serializes the object into a pretty-printed JSON string.
   *
   * @return A JSON formatted string representation of the object.
   */
  public String toJson() {
    // Use GsonBuilder to configure the output
    Gson gson =
        new GsonBuilder()
            .setPrettyPrinting() // This is the key to making it "pretty"
            .create();
    return gson.toJson(this);
  }
}
