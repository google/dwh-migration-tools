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

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabaseForConnection;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriver;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentHostUnlessUrl;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentJDBCUri;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPassword;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUser;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInputs;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractJdbcConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

/**
 * @author shevek
 */
@RespectsArgumentHostUnlessUrl
@RespectsArgumentUser
@RespectsArgumentPassword
@RespectsInputs({
  // Although RespectsInput is @Repeatable, errorprone fails on it.
  @RespectsInput(
      order = 450,
      arg = ConnectorArguments.OPT_ROLE,
      description = "The Snowflake role to use for authorization."),
  @RespectsInput(
      order = 500,
      arg = ConnectorArguments.OPT_WAREHOUSE,
      description = "The Snowflake warehouse to use for processing metadata queries.")
})
@RespectsArgumentDatabaseForConnection
@RespectsArgumentDriver
@RespectsArgumentJDBCUri
public abstract class AbstractSnowflakeConnector extends AbstractJdbcConnector {

  public AbstractSnowflakeConnector(@Nonnull String name) {
    super(name);
  }

  private static final int MAX_DATABASE_CHAR_LENGTH = 255;
  private static final String DEFAULT_DATABASE = "SNOWFLAKE";

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    String url = arguments.getUri();
    if (url == null) {
      StringBuilder buf = new StringBuilder("jdbc:snowflake://");
      String host = arguments.getHost("host.snowflakecomputing.com");
      buf.append(host).append("/");
      // FWIW we can/should totally use a Properties object here and pass it to
      // SimpleDriverDataSource rather than messing with the URL.
      List<String> optionalArguments = new ArrayList<>();
      if (arguments.getWarehouse() != null) {
        optionalArguments.add("warehouse=" + arguments.getWarehouse());
      }
      if (arguments.getRole() != null) {
        optionalArguments.add("role=" + arguments.getRole());
      }
      if (!optionalArguments.isEmpty()) {
        buf.append("?").append(Joiner.on("&").join(optionalArguments));
      }
      url = buf.toString();
    }
    String databaseName =
        arguments.getDatabases().isEmpty()
            ? DEFAULT_DATABASE
            : sanitizeDatabaseName(arguments.getDatabases().get(0));

    Driver driver =
        newDriver(arguments.getDriverPaths(), "net.snowflake.client.jdbc.SnowflakeDriver");
    String password = arguments.getPasswordIfFlagProvided().orElse(null);
    DataSource dataSource = new SimpleDriverDataSource(driver, url, arguments.getUser(), password);
    JdbcHandle jdbcHandle = new JdbcHandle(dataSource);
    setCurrentDatabase(databaseName, jdbcHandle.getJdbcTemplate());
    return jdbcHandle;
  }

  final ImmutableList<Task<?>> getSqlTasks(
      @Nonnull SnowflakeInput inputSource,
      @Nonnull AbstractJdbcTask<Summary> schemaTask,
      @Nonnull AbstractJdbcTask<Summary> usageTask) {
    switch (inputSource) {
      case USAGE_THEN_SCHEMA_SOURCE:
        return ImmutableList.of(usageTask, schemaTask.onlyIfFailed(usageTask));
      case SCHEMA_ONLY_SOURCE:
        return ImmutableList.of(schemaTask);
      case USAGE_ONLY_SOURCE:
        return ImmutableList.of(usageTask);
    }
    throw new AssertionError();
  }

  private void setCurrentDatabase(@Nonnull String databaseName, @Nonnull JdbcTemplate jdbcTemplate)
      throws MetadataDumperUsageException {
    String currentDatabase =
        jdbcTemplate.queryForObject(String.format("USE DATABASE %s;", databaseName), String.class);
    if (currentDatabase == null) {
      List<String> dbNames =
          jdbcTemplate.query("SHOW DATABASES", (rs, rowNum) -> rs.getString("name"));
      throw new MetadataDumperUsageException(
          "Database name not found "
              + databaseName
              + ", use one of: "
              + StringUtils.join(dbNames, ", "));
    }
  }

  String sanitizeDatabaseName(@Nonnull String databaseName) throws MetadataDumperUsageException {
    CharMatcher doubleQuoteMatcher = CharMatcher.is('"');
    String trimmedName = doubleQuoteMatcher.trimFrom(databaseName);
    int charLengthWithQuotes = databaseName.length() + 2;
    if (charLengthWithQuotes > 255) {
      throw new MetadataDumperUsageException(
          String.format(
              "The provided database name has %d characters, which is longer than the maximum"
                  + " allowed number %d for Snowflake identifiers.",
              charLengthWithQuotes, MAX_DATABASE_CHAR_LENGTH));
    }
    if (doubleQuoteMatcher.matchesAnyOf(trimmedName)) {
      throw new MetadataDumperUsageException(
          "Database name has incorrectly placed double quote(s). Aborting query.");
    }
    return trimmedName;
  }
}
