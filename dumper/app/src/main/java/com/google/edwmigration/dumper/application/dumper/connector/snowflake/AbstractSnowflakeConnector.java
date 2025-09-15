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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriver;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentHostUnlessUrl;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentJDBCUri;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPassword;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPrivateKeyFile;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPrivateKeyPassword;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUser;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInputs;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractJdbcConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

/** @author shevek */
@RespectsArgumentHostUnlessUrl
@RespectsArgumentUser
@RespectsArgumentPassword
@RespectsArgumentPrivateKeyFile
@RespectsArgumentPrivateKeyPassword
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
@RespectsArgumentDriver
@RespectsArgumentJDBCUri
public abstract class AbstractSnowflakeConnector extends AbstractJdbcConnector {

  public AbstractSnowflakeConnector(@Nonnull String name) {
    super(name);
  }

  @Nonnull
  @Override
  public abstract String getDescription();

  @Nonnull
  @Override
  public final Handle open(@Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException, SQLException {
    Properties properties = dataSourceProperties(arguments);
    String url = getUrlFromArguments(arguments);
    DataSource dataSource = new SimpleDriverDataSource(newDriver(arguments), url, properties);
    if (arguments.isAssessment()) {
      JdbcHandle handle = new JdbcHandle(dataSource);
      setCurrentDatabase("SNOWFLAKE", handle.getJdbcTemplate());
      return handle;
    } else {
      String databaseName =
          arguments.getDatabases().isEmpty()
              ? "SNOWFLAKE"
              : sanitizeDatabaseName(arguments.getDatabases().get(0));
      JdbcHandle handle = new JdbcHandle(dataSource);
      setCurrentDatabase(databaseName, handle.getJdbcTemplate());
      return handle;
    }
  }

  @Override
  public final void validate(@Nonnull ConnectorArguments arguments) {
    if (arguments.isPasswordFlagProvided() && arguments.isPrivateKeyFileProvided()) {
      String inconsistentAuth =
          "Private key authentication method can't be used together with user password. "
              + "If the private key file is encrypted, please use --"
              + ConnectorArguments.OPT_PRIVATE_KEY_PASSWORD
              + " to specify the key password.";
      throw new MetadataDumperUsageException(inconsistentAuth);
    }
    validateForConnector(arguments);
  }

  /**
   * Called by {@link #validate} to perform connector-specific checks.
   *
   * <p>Subclasses should override this with logic to run after the common validation for Snowflake.
   *
   * @param arguments User-provided arguments of the Dumper run.
   */
  protected abstract void validateForConnector(@Nonnull ConnectorArguments arguments);

  @Nonnull
  private Driver newDriver(@Nonnull ConnectorArguments arguments) throws SQLException {
    return newDriver(arguments.getDriverPaths(), "net.snowflake.client.jdbc.SnowflakeDriver");
  }

  @Nonnull
  private static Properties dataSourceProperties(@Nonnull ConnectorArguments arguments)
      throws SQLException {
    String user = arguments.getUserOrFail();
    if (arguments.isPrivateKeyFileProvided()) {
      return createPrivateKeyProperties(arguments, user);
    } else {
      return createUserPasswordProperties(arguments, user);
    }
  }

  private static Properties createUserPasswordProperties(
      @Nonnull ConnectorArguments arguments, @Nonnull String user) {
    Properties properties = new Properties();

    properties.put("user", user);
    if (arguments.isPasswordFlagProvided()) {
      properties.put("password", arguments.getPasswordOrPrompt());
    }
    // Set default authenticator only if url is not provided to allow user overriding it
    if (arguments.getUri() == null) {
      properties.put("authenticator", "username_password_mfa");
    }
    return properties;
  }

  private static Properties createPrivateKeyProperties(
      @Nonnull ConnectorArguments arguments, @Nonnull String user) {
    Properties properties = new Properties();
    properties.put("user", user);

    properties.put("private_key_file", arguments.getPrivateKeyFile());
    if (arguments.getPrivateKeyPassword() != null) {
      properties.put("private_key_pwd", arguments.getPrivateKeyPassword());
    }
    return properties;
  }

  @Nonnull
  private String getUrlFromArguments(@Nonnull ConnectorArguments arguments) {
    String url = arguments.getUri();
    if (url != null) {
      return url;
    }

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
    return buf.toString();
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
    int maxDatabaseCharLength = 255;
    if (charLengthWithQuotes > 255) {
      throw new MetadataDumperUsageException(
          String.format(
              "The provided database name has %d characters, which is longer than the maximum allowed number %d for Snowflake identifiers.",
              charLengthWithQuotes, maxDatabaseCharLength));
    }
    if (doubleQuoteMatcher.matchesAnyOf(trimmedName)) {
      throw new MetadataDumperUsageException(
          "Database name has incorrectly placed double quote(s). Aborting query.");
    }
    return trimmedName;
  }

  static String describeAsDelegate(Connector connector, String baseName) {
    String summary = String.format("* %s - %s\n", connector.getName(), connector.getDescription());
    String details = String.format("%8s[same options as '%s']\n", "", baseName);
    return summary + details;
  }

  static String columnOf(Enum<?> enumValue) {
    String name = enumValue.name();
    return UPPER_CAMEL.to(UPPER_UNDERSCORE, name);
  }
}
