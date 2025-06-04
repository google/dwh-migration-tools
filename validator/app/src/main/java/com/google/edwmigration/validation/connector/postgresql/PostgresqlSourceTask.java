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
package com.google.edwmigration.validation.connector.postgresql;

import com.google.edwmigration.validation.config.ValidationType;
import com.google.edwmigration.validation.connector.common.AbstractSourceTask;
import com.google.edwmigration.validation.connector.jdbc.JdbcHandle;
import com.google.edwmigration.validation.io.writer.ResultSetWriterFactory;
import com.google.edwmigration.validation.model.ExecutionState;
import java.sql.Connection;
import java.util.HashMap;
import javax.sql.DataSource;
import org.jooq.DataType;
import org.jooq.SQLDialect;

public class PostgresqlSourceTask extends AbstractSourceTask {
  public PostgresqlSourceTask(ExecutionState state, ResultSetWriterFactory writerFactory) {
    super(state, writerFactory);
  }

  @Override
  public void run() throws Exception {
    JdbcHandle pg = (JdbcHandle) getHandle();
    DataSource ds = pg.getDataSource();
    try (Connection connection = ds.getConnection()) {
      PostgresqlSqlGenerator generator = new PostgresqlSqlGenerator(SQLDialect.POSTGRES, context);
      String numericColsQuery = generator.getNumericColumnsQuery(context.sourceTable);
      HashMap<String, DataType<? extends Number>> numericCols =
          executeNumericColsQuery(connection, generator, numericColsQuery);
      String aggregateQuery =
          generator.getAggregateQuery(
              numericCols,
              context.sourceTable.getFullyQualifiedTable(),
              context.sourceTable.name,
              context.bqTargetTable.name,
              true);
      String rowSampleQuery =
          generator.getRowSampleQuery(
              context.sourceTable.getFullyQualifiedTable(), context.sourceTable.primaryKey);

      setAggregateQueryMetadata(
          extractQueryResults(
              context.sourceTable.name, connection, aggregateQuery, ValidationType.AGGREGATE));
      setRowQueryMetadata(
          extractQueryResults(
              context.sourceTable.name, connection, rowSampleQuery, ValidationType.ROW));
    }
  }
}
