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
package com.google.edwmigration.dumper.application.dumper.connector.oracle.task;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public interface GroupTask extends Task<Summary> {

  @CheckForNull
  public Exception getException();

  /**
   * Create a task running a "SELECT" query.
   *
   * <p>The query is a simple select with no filtering.
   */
  @Nonnull
  static GroupTask createSelect(String outputFile, String selectQuery) {
    return new SelectTask(outputFile, selectQuery);
  }

  /** Create a task running a "SELECT" query with filtering. */
  @Nonnull
  static GroupTask createSelect(String outputFile, String selectQuery, String where) {
    checkArgument(where.startsWith(" "));
    return new SelectTask(outputFile, selectQuery + where);
  }

  /** Create a task running "SELECT *" on the specified table. */
  @Nonnull
  static GroupTask createSelectStar(String outputFile, String table) {
    return new SelectTask(outputFile, "SELECT * FROM " + table);
  }

  /** Create a task running "SELECT *" on the specified table with filtering. */
  @Nonnull
  static GroupTask createSelectStar(String outputFile, String table, String where) {
    checkArgument(where.startsWith(" "));
    return new SelectTask(outputFile, "SELECT * FROM " + table + where);
  }
}
