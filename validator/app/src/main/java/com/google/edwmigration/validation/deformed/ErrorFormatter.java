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

import java.util.Map;

public class ErrorFormatter {

  public static String format(ValidationState state) {
    StringBuilder sb = new StringBuilder();

    for (Map.Entry<String, FieldValidationState> entry : state.getAll().entrySet()) {
      String field = entry.getKey();
      FieldValidationState fieldState = entry.getValue();

      if (!fieldState.isValid) {
        sb.append("❌ ").append(field).append(" failed validation:\n");
        // sb.append(fieldState.toJson());
        for (String error : fieldState.errors) {
          sb.append("   - ").append(error).append("\n");
        }
      }
    }

    return sb.toString();
  }

  public static String log(ValidationState state) {
    String json = state.toJson();
    return json;
  }
}
