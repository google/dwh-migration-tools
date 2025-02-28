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

import com.google.edwmigration.validation.application.validator.ValidationArguments;
import com.google.edwmigration.validation.application.validator.handle.Handle;
import com.google.edwmigration.validation.application.validator.handle.JdbcHandle;
import com.google.edwmigration.validation.application.validator.task.AbstractJdbcTask;
import com.google.edwmigration.validation.application.validator.task.AbstractTask;
import com.google.edwmigration.validation.application.validator.sql.AbstractSourceSqlGenerator;
import java.net.URI;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.jooq.SQLDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class PostgresqlValidationConnector extends PostgresqlAbstractConnector {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresqlValidationConnector.class);

  public PostgresqlValidationConnector() {
    super("postgresql");
  }

  public class PostgresqlTask extends AbstractJdbcTask {
    public PostgresqlTask(Handle handle, URI outputUri, ValidationArguments arguments) {
      super(handle, outputUri, arguments);
    }

    class PostgresqlSourceSqlGenerator extends AbstractSourceSqlGenerator {

      public PostgresqlSourceSqlGenerator() {
        super(
            SQLDialect.POSTGRES,
            getArguments().getTable(),
            getArguments().getOptConfidenceInterval());
      }

      @Override
      public List<String> getNumericColumns() {
        return Arrays.asList("id", "salary");
      }

      public String getPrimaryKey() {
        return "id";
      }

      public Long getRowCount() {
        return 49L;
      }
    }

    @Override
    public void run() throws Exception {
      PostgresqlSourceSqlGenerator generator = new PostgresqlSourceSqlGenerator();
      String aggregateQuery = generator.getAggregateQuery();
      String rowSampleQuery = generator.getRowSampleQuery();

      JdbcHandle pg = (JdbcHandle) getHandle();
      DataSource ds = pg.getDataSource();

      try (Connection connection = ds.getConnection()) {
        LOG.debug("Connected to " + connection);
        doInConnection(connection, aggregateQuery, QueryType.AGGREGATE);
        doInConnection(connection, rowSampleQuery, QueryType.ROW);
      }
    }
  }

  @Nonnull
  @Override
  public AbstractTask getSourceQueryTask(
      Handle handle, URI outputUri, ValidationArguments arguments) {
    return new PostgresqlTask(handle, outputUri, arguments);
  }
}
