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
package com.google.edwmigration.validation.model;

import com.google.edwmigration.validation.deformed.ValidationProperty;
import java.util.List;
import java.util.Map;

public class TableSchema {
  /*
   * tableType must be `SourceTable` or `BqTargetTable`
   */
  public static <T extends Table> Map<String, List<ValidationProperty<T>>> injectTableRules(
      String tableType) {
    if (!("SourceTable".contentEquals(tableType) || "BqTargetTable".contentEquals(tableType))) {
      throw new IllegalArgumentException(
          "tableType must be SourceTable or BqTargetTable. Was: " + tableType);
    }

    return Map.of(
        "primaryKey",
        List.of(
            ValidationProperty.of(
                "`primaryKey` is required in [" + tableType + "] of TOML.",
                s -> s.primaryKey != null && !s.primaryKey.trim().isEmpty())),
        "schema",
        List.of(
            ValidationProperty.of(
                "`schema` is required in [" + tableType + "] of TOML.",
                s -> s.schema != null && !s.schema.trim().isEmpty())),
        "table",
        List.of(
            ValidationProperty.of(
                "`name` is required in [" + tableType + "] of TOML.",
                s -> s.name != null && !s.name.trim().isEmpty())));
  }
}
