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
package com.google.edwmigration.validation.application.validator.connector.postgresql;

import static org.jooq.impl.DSL.*;

import com.google.edwmigration.validation.application.validator.ValidationArguments;
import com.google.edwmigration.validation.application.validator.handle.Handle;
import com.google.edwmigration.validation.application.validator.handle.JdbcHandle;
import com.google.edwmigration.validation.application.validator.task.AbstractJdbcSourceTask;
import com.google.edwmigration.validation.application.validator.task.AbstractSourceTask;
import com.google.edwmigration.validation.application.validator.task.AbstractTargetTask;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.apache.commons.lang3.NotImplementedException;
import org.jooq.DataType;
import org.jooq.SQLDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class PostgresqlValidationConnector extends PostgresqlAbstractConnector {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresqlValidationConnector.class);

  public PostgresqlValidationConnector() {
    super("postgresql");
  }

  public static class PostgresqlSourceTask extends AbstractJdbcSourceTask {
    public PostgresqlSourceTask(Handle handle, URI outputUri, ValidationArguments arguments) {
      super(handle, outputUri, arguments);
    }

    @Override
    public void run() throws Exception {
      JdbcHandle pg = (JdbcHandle) getHandle();
      DataSource ds = pg.getDataSource();

      try (Connection connection = ds.getConnection()) {
        LOG.debug("Connected to " + connection);
        PostgresqlSourceSqlGenerator generator =
            new PostgresqlSourceSqlGenerator(
                SQLDialect.POSTGRES,
                getArguments().getTable().getSource(),
                getArguments().getOptConfidenceInterval(),
                getArguments().getColumnMappings());
        String numericColsQuery = generator.getNumericColumnsQuery();
        HashMap<String, DataType<? extends Number>> numericCols =
            executeNumericColsQuery(connection, generator, numericColsQuery);
        LOG.debug(String.valueOf(numericCols));

        String aggregateQuery = generator.getAggregateQuery(numericCols);
        LOG.debug(aggregateQuery);
        String rowSampleQuery = generator.getRowSampleQuery();

        ResultSetMetaData aggregateMetadata =
            doInConnection(connection, aggregateQuery, QueryType.AGGREGATE);
        setAggregateQueryMetadata(aggregateMetadata);
        ResultSetMetaData rowMetadata = doInConnection(connection, rowSampleQuery, QueryType.ROW);
        setRowQueryMetadata(rowMetadata);
      }
    }
  }

  @Nonnull
  @Override
  public AbstractSourceTask getSourceQueryTask(
      Handle handle, URI outputUri, ValidationArguments arguments) {
    return new PostgresqlSourceTask(handle, outputUri, arguments);
  }

  @Nonnull
  @Override
  public AbstractTargetTask getTargetQueryTask(
      Handle handle, URI outputUri, ValidationArguments arguments) {
    throw new NotImplementedException("PostgreSQL as a target is not implemented.");
  }
}
