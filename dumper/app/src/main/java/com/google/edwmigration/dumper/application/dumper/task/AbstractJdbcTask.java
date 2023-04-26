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
package com.google.edwmigration.dumper.application.dumper.task;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Base64;
import javax.annotation.CheckForNull;
import javax.annotation.CheckForSigned;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;

/** @author shevek */
public abstract class AbstractJdbcTask<T> extends AbstractTask<T> {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcTask.class);

  @CheckForNull private Class<? extends Enum<?>> headerClass;

  public AbstractJdbcTask(@Nonnull String targetPath) {
    super(targetPath);
  }

  @CheckForNull
  public Class<? extends Enum<?>> getHeaderClass() {
    return headerClass;
  }

  @Nonnull
  public AbstractJdbcTask<T> withHeaderClass(@Nonnull Class<? extends Enum<?>> headerClass) {
    this.headerClass = headerClass;
    return this;
  }

  @Nonnull
  protected CSVFormat newCsvFormat(@Nonnull ResultSet rs) throws SQLException {
    CSVFormat format = FORMAT;
    Class<? extends Enum<?>> headerClass = getHeaderClass();
    if (headerClass != null) {
      format = format.withHeader(headerClass);
      if (headerClass.getEnumConstants().length != rs.getMetaData().getColumnCount())
        // Can we avoid nesting exceptions here?
        throw new SQLException(
            new MetadataDumperUsageException(
                "Fatal Error. ResultSet does not have the expected column count: "
                    + headerClass.getEnumConstants().length,
                Arrays.asList(
                    "If a custom query has been specified please confirm the selected columns match"
                        + " the following: ",
                    StringUtils.join(headerClass.getEnumConstants(), ", "))));
    } else {
      format = format.withHeader(rs);
    }
    return format;
  }

  @Nonnull
  protected ExtendableResultSetExtractor newCsvResultSetExtractor(
      @Nonnull ByteSink sink, @CheckForSigned long count) {
    return rs -> {
      CSVFormat format = newCsvFormat(rs);
      try (RecordProgressMonitor monitor =
              count >= 0
                  ? new RecordProgressMonitor(getName(), count)
                  : new RecordProgressMonitor(getName());
          Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
          CSVPrinter printer = format.print(writer)) {
        final int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
          monitor.count();
          for (int i = 1; i <= columnCount; i++) {
            Object object = rs.getObject(i);
            if (object instanceof byte[]) {
              printer.print(Base64.getEncoder().encodeToString((byte[]) object));
            } else if (object instanceof Clob) {
              InputStream in = ((Clob) object).getAsciiStream();
              StringWriter w = new StringWriter();
              IOUtils.copy(in, w);
              printer.print(w.toString());
            } else {
              printer.print(object);
            }
          }
          printer.println();
        }
        return new Summary(monitor.getCount());
      } catch (IOException e) {
        throw new SQLException(e);
      }
    };
  }

  public interface ExtendableResultSetExtractor extends ResultSetExtractor<Summary> {
    default ResultSetExtractor<Summary> withInterval(ZonedInterval interval) {
      return rs -> extractData(rs).withInterval(interval);
    }
  }

  public static void setParameterValues(@Nonnull PreparedStatement statement, Object... arguments)
      throws SQLException {
    for (int i = 0; i < arguments.length; i++)
      StatementCreatorUtils.setParameterValue(
          statement, i + 1, SqlTypeValue.TYPE_UNKNOWN, arguments[i]);
  }

  @SuppressWarnings("UnusedMethod")
  private static void debug(@Nonnull Statement statement) throws SQLException {
    LOG.debug(
        "Concurrency = "
            + statement.getResultSetConcurrency()
            + " (want "
            + ResultSet.CONCUR_READ_ONLY
            + ")");
    LOG.debug(
        "Holdability = "
            + statement.getResultSetHoldability()
            + " (want "
            + ResultSet.CLOSE_CURSORS_AT_COMMIT
            + ")");
    LOG.debug("FetchSize = " + statement.getFetchSize());
    LOG.debug(
        "ResultSetType = "
            + statement.getResultSetType()
            + " (want "
            + ResultSet.TYPE_FORWARD_ONLY
            + ")");
    LOG.debug("AutoCommit = " + statement.getConnection().getAutoCommit() + " (want false)");
  }

  // Very similar to JdbcTemplate, except works for bulk selects without blowing RAM.
  @CheckForNull
  protected static <T> T doSelect(
      @Nonnull Connection connection,
      @Nonnull ResultSetExtractor<T> resultSetExtractor,
      @Nonnull String sql,
      @Nonnull Object... arguments)
      throws SQLException {
    PreparedStatement statement = null;
    try {
      LOG.debug("Preparing statement...");

      PREPARE:
      {
        Stopwatch stopwatch = Stopwatch.createStarted();
        // Causes PostgreSQL to use cursors, rather than RAM.
        // https://jdbc.postgresql.org/documentation/83/query.html#fetchsize-example
        // https://medium.com/@FranckPachot/oracle-postgres-jdbc-fetch-size-3012d494712
        connection.setAutoCommit(false);
        // connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);    // Shouldn't be
        // required.
        statement =
            connection.prepareStatement(
                sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        setParameterValues(statement, arguments);
        // statement.setFetchDirection(ResultSet.FETCH_FORWARD);   // PostgreSQL and (allegedly)
        // Teradata prefer this. However, it is the default, and sqlite throws.
        // Enables cursors in PostgreSQL.
        // Teradata says that this can reduce the fetch size below 1Mb, but not increase it.
        statement.setFetchSize(16384);
        LOG.debug("Statement preparation took " + stopwatch + ". Executing...");
      }

      EXECUTE:
      {
        // debug(statement);
        Stopwatch stopwatch = Stopwatch.createStarted();
        statement.execute(); // Must return true to indicate a ResultSet object.
        LOG.debug("Statement execution took " + stopwatch + ". Extracting results...");
        // debug(statement);
      }

      T result = null;
      ResultSet rs = null;
      try {
        Stopwatch stopwatch = Stopwatch.createStarted();
        rs = statement.getResultSet();
        result = resultSetExtractor.extractData(rs);
        LOG.debug("Result set extraction took " + stopwatch + ".");
      } finally {
        JdbcUtils.closeResultSet(rs);
      }

      SQLWarning warning = statement.getWarnings();
      while (warning != null) {
        LOG.warn(
            "SQL warning: ["
                + warning.getSQLState()
                + "/"
                + warning.getErrorCode()
                + "] "
                + warning.getMessage());
        warning = warning.getNextWarning();
      }

      return result;
    } finally {
      JdbcUtils.closeStatement(statement);
    }
  }

  @CheckForNull
  protected abstract T doInConnection(
      @Nonnull TaskRunContext context,
      @Nonnull JdbcHandle jdbcHandle,
      @Nonnull ByteSink sink,
      @Nonnull Connection connection)
      throws SQLException;

  @Override
  protected T doRun(TaskRunContext context, ByteSink sink, Handle handle) throws Exception {
    JdbcHandle jdbcHandle = (JdbcHandle) handle;
    LOG.info("Writing to " + getTargetPath() + " -> " + sink);

    DataSource dataSource = jdbcHandle.getDataSource();
    // We could use JdbcUtils, but that would prevent us from getting a .exception.txt.
    try (Connection connection = DataSourceUtils.getConnection(dataSource)) {
      // LOG.debug("Connected to " + connection); // Hikari is using the same connection each time.
      return doInConnection(context, jdbcHandle, sink, connection);
    }
  }

  @Override
  public String toString() {
    // This should be overridden by anyone serious.
    // Perhaps we should declare this abstract instead and force the point.
    return "Write " + getTargetPath() + " from " + getClass().getSimpleName();
  }
}
