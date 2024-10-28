/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import javax.annotation.ParametersAreNonnullByDefault;

@AutoValue
@ParametersAreNonnullByDefault
abstract class TaskVariant {

  public abstract String zipEntryName();

  public abstract String schemaName();

  public abstract String whereClause();

  static TaskVariant createWithFilter(String zipEntryName, String schemaName, String whereClause) {
    Preconditions.checkArgument(!whereClause.isEmpty(), "Provided WHERE clause was empty");
    return create(zipEntryName, schemaName, whereClause);
  }

  static TaskVariant createWithNoFilter(String zipEntryName, String schemaName) {
    return create(zipEntryName, schemaName, /* whereClause */ "");
  }

  private static TaskVariant create(String zipEntryName, String schemaName, String whereClause) {
    return new AutoValue_TaskVariant(zipEntryName, schemaName, whereClause);
  }
}
