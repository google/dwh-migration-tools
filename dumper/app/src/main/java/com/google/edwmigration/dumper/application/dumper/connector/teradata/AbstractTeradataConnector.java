/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriverClass;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriverRequired;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentHostUnlessUrl;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPassword;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUri;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUser;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractJdbcConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.core.SingleColumnRowMapper;

/**
 * Note that connecting to any Teradata database requires their JDBC driver (not included here for
 * obvious reasons). For details on how to obtain and use this driver, see the asciidoc
 * documentation entitled "Metadata and Query Log Dumper"
 *
 * <p>Teradata table references:
 * http://elsasoft.com/samples/teradata/Teradata.127.0.0.1.DBC/allTables.htm
 * https://docs.teradata.com/reader/B7Lgdw6r3719WUyiCSJcgw/zpnjJYg3OPoAeMcGbnKuNQ
 *
 * @author matt
 */
@RespectsArgumentHostUnlessUrl
@RespectsInput(
    order = ConnectorArguments.OPT_PORT_ORDER,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the server.",
    required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
    defaultValue = "" + AbstractTeradataConnector.OPT_PORT_DEFAULT)
@RespectsArgumentUser
@RespectsArgumentPassword
@RespectsArgumentDriverRequired
@RespectsArgumentUri
@RespectsArgumentDriverClass
public abstract class AbstractTeradataConnector extends AbstractJdbcConnector {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTeradataConnector.class);

  public static final int OPT_PORT_DEFAULT = 1025;
  protected static final DateTimeFormatter SQL_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);
  @VisibleForTesting /* pp */ static final String DEF_LOG_TABLE = "dbc.DBQLogTbl";
  @VisibleForTesting /* pp */ static final String DEF_QUERY_TABLE = "dbc.DBQLSQLTbl";

  protected static class TeradataJdbcSelectTask extends JdbcSelectTask {

    private final String sqlCount;
    private final TaskCategory category;

    public TeradataJdbcSelectTask(String targetPath, TaskCategory category, String sqlTemplate) {
      super(targetPath, String.format(sqlTemplate, "*"));
      this.sqlCount = sqlTemplate.contains("%s") ? String.format(sqlTemplate, "count(*)") : null;
      this.category = Preconditions.checkNotNull(category);
    }

    @Override
    public TaskCategory getCategory() {
      return category;
    }

    // This works to execute the count SQL on the same connection as the data SQL,
    // because it's called from doInConnection, when we already have one connection open.
    @Nonnull
    private ResultSetExtractor<Summary> newCountedResultSetExtractor(
        @Nonnull ByteSink sink, @Nonnull Connection connection) throws SQLException {
      long count = -1;
      if (sqlCount != null) {
        // It's a lot of infrastructure, but we don't have to write it.
        RowMapper<Long> rowMapper = new SingleColumnRowMapper<>(Long.class);
        ResultSetExtractor<List<Long>> resultSetExtractor =
            new RowMapperResultSetExtractor<>(rowMapper);
        List<Long> results = doSelect(connection, resultSetExtractor, sqlCount);
        Long result = DataAccessUtils.nullableSingleResult(results);
        if (result != null) count = result;
      }
      return newCsvResultSetExtractor(sink, count);
    }

    @Override
    protected Summary doInConnection(
        TaskRunContext context, JdbcHandle jdbcHandle, ByteSink sink, Connection connection)
        throws SQLException {
      try {
        connection.setAutoCommit(false);
        PreparedStatement statement =
            connection.prepareStatement(
                "SET QUERY_BAND='ApplicationName=compilerworks;' FOR SESSION");
        statement.execute();
      } catch (SQLException e) {
        // We might still be in postgresql or sqlite.
        LOG.warn("Failed to set QUERY_BAND: " + e);
        // This puts the transaction in an aborted state unless we rollback here.
        connection.rollback();
      }
      ResultSetExtractor<Summary> rse = newCountedResultSetExtractor(sink, connection);
      return doSelect(connection, rse, getSql());
    }
  }

  /** This is shared between all instances of TeradataLogsJdbcTask. */
  protected static class SharedState {
    /**
     * Whether a particular expression is valid against the particular target Teradata version. This
     * is a concurrent Map of immutable objects, so is threadsafe overall.
     */
    protected final ConcurrentMap<String, Boolean> expressionValidity = new ConcurrentHashMap<>();
  }

  protected static boolean isQueryTable(@Nonnull String expression) {
    return expression.startsWith("ST.");
  }

  /* pp */ AbstractTeradataConnector(@Nonnull String name) {
    super(name);
  }

  @Nonnull
  @Override
  public Handle open(ConnectorArguments arguments) throws Exception {
    String url = arguments.getUri();
    if (url == null) {
      String host = arguments.getHost();
      int port = arguments.getPort(OPT_PORT_DEFAULT);
      url = "jdbc:teradata://" + host + "/DBS_PORT=" + port + ",TMODE=ANSI,CHARSET=UTF8";
      // ,MAX_MESSAGE_BODY=16777216
    }

    Driver driver =
        newDriver(
            arguments.getDriverPaths(), arguments.getDriverClass("com.teradata.jdbc.TeraDriver"));
    DataSource dataSource = newSimpleDataSource(driver, url, arguments);
    // Teradata has a hard connection limit; let's stay below it, and block threads if required.
    // This could probably be 1 because the Teradata dumper is single-threaded.
    return JdbcHandle.newPooledJdbcHandle(dataSource, 2);
  }
}
