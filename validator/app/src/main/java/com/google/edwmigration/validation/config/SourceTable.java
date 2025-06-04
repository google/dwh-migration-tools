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
package com.google.edwmigration.validation.config;

import com.google.edwmigration.validation.deformed.ValidationProperty;
import com.google.edwmigration.validation.deformed.ValidationSchema;
import com.google.edwmigration.validation.model.Table;
import com.google.edwmigration.validation.model.TableSchema;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SourceTable extends Table {
  public static final ValidationSchema<SourceTable> buildSchema() {
    Map<String, List<ValidationProperty<SourceTable>>> sourceMap = new HashMap<>();
    sourceMap.putAll(TableSchema.injectTableRules("BqTargetTable"));
    return new ValidationSchema<>(sourceMap);
  }
  ;

  public SourceTable() {}

  public SourceTable(String schema, String table, String primaryKey) {
    super(schema, table, primaryKey);
  }
}
