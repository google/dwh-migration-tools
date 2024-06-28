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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriverRequired;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentHostUnlessUrl;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPassword;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUri;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUser;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInputs;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractJdbcConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorPropertyWithDefault;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.utils.PropertyParser;
import java.sql.Driver;
import java.time.Clock;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

/**
 * https://docs.oracle.com/cd/B28359_01/java.111/b31224/getsta.htm You must set the CLASSPATH
 * environment variable for your installed JDBC OCI or Thin driver. Include the following in the
 * CLASSPATH environment variable:
 *
 * <p>ORACLE_HOME/jdbc/lib/ojdbc5.jar ORACLE_HOME/jlib/orai18n.jar
 * https://www.logicbig.com/tutorials/spring-framework/spring-data-access-with-jdbc/connect-oracle.html
 *
 * <p>https://www.oracle.com/technetwork/database/application-development/jdbc-eecloud-3089380.html
 *
 * <p>Recommended Oracle way to dump code: <code>
 * expdp directory={oracle_directory} content=metadata_only full=y dumpfile={dumpfilename} | impdp directory={oracle_directory} content=metadata_only full=y dumpfile ={dumpfile} sqlfile=ddl.sql
 * </code> but this needs to be done by folks who have access to the oracle server given <code>
 * directory={oracle_directory}, dumpfile={dumpfilename}</code> are on the same physical server that
 * Oracle is running on.
 */
@RespectsArgumentDriverRequired
@RespectsArgumentHostUnlessUrl
@RespectsInput(
    order = ConnectorArguments.OPT_PORT_ORDER,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the server.",
    required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
    defaultValue = "" + AbstractOracleConnector.OPT_PORT_DEFAULT)
@RespectsArgumentUser
@RespectsArgumentPassword
@RespectsArgumentUri
@RespectsInputs({
  // Although RespectsInput is @Repeatable, errorprone fails on it.
  @RespectsInput(
      order = 450,
      arg = ConnectorArguments.OPT_ORACLE_SID,
      description = "The Oracle SID for JDBC onnection"),
  @RespectsInput(
      order = 500,
      arg = ConnectorArguments.OPT_ORACLE_SERVICE,
      description = "The Oracle Service name for JDBC connection")
})
public abstract class AbstractOracleConnector extends AbstractJdbcConnector {

  public static final int OPT_PORT_DEFAULT = 1521;

  private final OracleConnectorScope connectorScope;

  protected enum CommonOracleConnectorProperty implements ConnectorPropertyWithDefault {
    USE_FETCH_SIZE_WITH_LONG_COLUMN(
        "oracle.use-fetch-size-with-long-column",
        "Enables prefetch of rows with a LONG or LONG RAW column. This parameter improves the query performance"
            + " but can result in higher memory consumption. Default value: \"true\", set to \"false\" to disable it.",
        "true");

    private final String name;
    private final String description;
    private final Optional<String> defaultValue;

    CommonOracleConnectorProperty(String name, String description) {
      this.name = name;
      this.description = description;
      this.defaultValue = Optional.empty();
    }

    CommonOracleConnectorProperty(String name, String description, String defaultValue) {
      this.name = name;
      this.description = description;
      this.defaultValue = Optional.of(defaultValue);
    }

    @Nonnull
    public String getName() {
      return name;
    }

    @Nonnull
    public String getDescription() {
      return description;
    }

    public String getDefaultValue() {
      return defaultValue.orElse("");
    }
  }

  @Nonnull
  @Override
  public Class<? extends Enum<? extends ConnectorPropertyWithDefault>> getConnectorProperties() {
    return CommonOracleConnectorProperty.class;
  }

  public AbstractOracleConnector(@Nonnull OracleConnectorScope connectorScope) {
    super(connectorScope.connectorName());
    this.connectorScope = connectorScope;
  }

  @Override
  @Nonnull
  public String getDefaultFileName(boolean isAssessment, Clock clock) {
    return connectorScope.toFileName(isAssessment, clock);
  }

  @Nonnull
  String getFormatName() {
    return connectorScope.formatName();
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    Driver driver = newDriver(arguments.getDriverPaths(), "oracle.jdbc.OracleDriver");
    DataSource dataSource =
        new SimpleDriverDataSource(driver, buildUrl(arguments), buildProperties(arguments));
    return new JdbcHandle(dataSource);
  }

  @Nonnull
  OracleConnectorScope getConnectorScope() {
    return connectorScope;
  }

  @Nonnull
  Properties buildProperties(@Nonnull ConnectorArguments arguments) {
    Properties properties = new Properties();
    properties.setProperty("user", arguments.getUserOrFail());
    properties.setProperty("password", arguments.getPasswordOrPrompt());
    properties.setProperty(
        "useFetchSizeWithLongColumn",
        PropertyParser.getString(
                arguments, CommonOracleConnectorProperty.USE_FETCH_SIZE_WITH_LONG_COLUMN)
            .orElse(
                CommonOracleConnectorProperty.USE_FETCH_SIZE_WITH_LONG_COLUMN.getDefaultValue()));

    return properties;
  }

  // dbc:oracle:thin://<host>:<port>/<service>
  // jdbc:oracle:thin:<host>:<port>:<SID>
  // jdbc:oracle:thin:<TNSName> (from 10.2.0.1.0)
  static String buildUrl(ConnectorArguments arguments) {
    String url = arguments.getUri();
    String host = arguments.getHost();
    int port = arguments.getPort(OPT_PORT_DEFAULT);
    if (url != null) {
      checkNonUriFlags(arguments);
      return url;
    }
    checkServiceAndSid(arguments);
    if (arguments.getOracleSID() != null) {
      return "jdbc:oracle:thin:@" + host + ":" + port + ":" + arguments.getOracleSID();
    } else {
      return "jdbc:oracle:thin:@//" + host + ":" + port + "/" + arguments.getOracleServicename();
    }
  }

  private static void checkNonUriFlags(ConnectorArguments arguments) {
    if (arguments.getHost() != null) {
      throw extraFlagProvided("host");
    } else if (arguments.getPort() != null) {
      throw extraFlagProvided("port");
    } else if (arguments.getOracleServicename() != null) {
      throw extraFlagProvided("oracle-service");
    } else if (arguments.getOracleSID() != null) {
      throw extraFlagProvided("oracle-sid");
    }
  }

  private static void checkServiceAndSid(ConnectorArguments arguments) {
    boolean hasService = arguments.getOracleServicename() != null;
    boolean hasSid = arguments.getOracleSID() != null;
    if (!hasService && !hasSid || hasService && hasSid) {
      throw new MetadataDumperUsageException(
        "Provide either -oracle-service or -oracle-sid for oracle dumper");
    }
  }

  private static MetadataDumperUsageException extraFlagProvided(String flagName) {
    String message =
        String.format(
            "Both url and %s were provided. If the url is valid, please omit the %s",
            flagName, flagName);
    return new MetadataDumperUsageException(message);
  }
}
