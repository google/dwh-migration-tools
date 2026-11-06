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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.client;

import com.google.common.collect.ImmutableList;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Executes SQL statements on a SQL warehouse ({@link DatabricksClientSqlRunner} or {@link
 * JdbcSqlRunner}) or on an all-purpose cluster ({@link JdbcSqlRunner}). The calls throw the {@code
 * DatabricksException} of the SDK when the API returns an error, and an {@link IOException} when a
 * statement fails.
 */
public interface DatabricksSqlRunner extends Closeable {

  /** Executes {@code sql} in {@code catalog} and returns its result rows. */
  StatementResult executeSqlInCatalog(String catalog, String sql) throws IOException;

  /**
   * The result rows of a SQL statement, read one row at a time, with each cell as text. Closing the
   * result releases any underlying statement or pooled connection.
   */
  interface StatementResult extends Closeable {

    /**
     * Returns the cell at {@code index}, or null if {@code row} is null or {@code index} is out of
     * bounds.
     */
    static String cell(List<String> row, int index) {
      return row == null || index < 0 || index >= row.size() ? null : row.get(index);
    }

    /** Returns the names of the result columns, in order. */
    ImmutableList<String> columnNames();

    /** Returns the next result row, or null at the end. */
    List<String> nextRow() throws IOException;

    @Override
    default void close() throws IOException {}
  }
}
