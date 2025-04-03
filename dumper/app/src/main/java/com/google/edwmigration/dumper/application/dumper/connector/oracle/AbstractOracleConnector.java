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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_HOST;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_HOST_DEFAULT;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_ORACLE_SERVICE;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_ORACLE_SID;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_PORT;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_QUERY_LOG_DAYS;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_URI;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.AssessmentSupport.REQUIRED;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.AssessmentSupport.UNSUPPORTED;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriverRequired;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentHostUnlessUrl;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentJDBCUri;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPassword;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUser;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInputs;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractJdbcConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorPropertyWithDefault;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.utils.PropertyParser;
import java.io.IOException;
import java.sql.Driver;
import java.sql.SQLRecoverableException;
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
@RespectsArgumentJDBCUri
@RespectsInputs({
  @RespectsInput(
      arg = OPT_QUERY_LOG_DAYS,
      description = "The number of days of query history to dump."),
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
    validateAssessmentFlag(arguments.isAssessment());
    Driver driver = newDriver(arguments.getDriverPaths(), "oracle.jdbc.OracleDriver");
    String url = buildUrl(arguments);
    DataSource dataSource = new SimpleDriverDataSource(driver, url, buildProperties(arguments));
    try {
      return new JdbcHandle(dataSource);
    } catch (SQLRecoverableException e) {
      throw new IOException(
          String.format(
              "Failed connecting to the Oracle database on %s.\n"
                  + "Check if the connection attributes are correct and try again.\n"
                  + "For more details check the troubleshooting section: "
                  + "https://cloud.google.com/bigquery/docs/generate-metadata#oracle_connection_issue",
              url),
          e);
    }
  }

  private void validateAssessmentFlag(boolean isAssessment) {
    if (assessmentSupport().equals(REQUIRED) && !isAssessment) {
      throw noAssessmentException();
    } else if (assessmentSupport().equals(UNSUPPORTED) && isAssessment) {
      throw unsupportedAssessmentException();
    }
  }

  @Nonnull
  public abstract AssessmentSupport assessmentSupport();

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
    if (url != null) {
      checkNonUriFlags(arguments);
      return url;
    }
    checkServiceAndSid(arguments);
    String host = arguments.getHost(OPT_HOST_DEFAULT);
    int port = arguments.getPort(OPT_PORT_DEFAULT);
    if (arguments.getOracleSID() != null) {
      return "jdbc:oracle:thin:@" + host + ":" + port + ":" + arguments.getOracleSID();
    } else {
      return "jdbc:oracle:thin:@//" + host + ":" + port + "/" + arguments.getOracleServicename();
    }
  }

  private static void checkNonUriFlags(ConnectorArguments arguments) {
    if (arguments.getHost() != null) {
      throw extraFlagProvided(OPT_HOST);
    } else if (arguments.getPort() != null) {
      throw extraFlagProvided(OPT_PORT);
    } else if (arguments.getOracleServicename() != null) {
      throw extraFlagProvided(OPT_ORACLE_SERVICE);
    } else if (arguments.getOracleSID() != null) {
      throw extraFlagProvided(OPT_ORACLE_SID);
    }
  }

  private static void checkServiceAndSid(ConnectorArguments arguments) {
    boolean hasService = arguments.getOracleServicename() != null;
    boolean hasSid = arguments.getOracleSID() != null;
    if ((!hasService && !hasSid) || (hasService && hasSid)) {
      String message =
          String.format(
              "Provide either --%s or --%s for oracle dumper", OPT_ORACLE_SERVICE, OPT_ORACLE_SID);
      throw new MetadataDumperUsageException(message);
    }
  }

  private static MetadataDumperUsageException extraFlagProvided(String flagName) {
    String[] sentences =
        new String[] {
          String.format("Both the --%s and --%s flags were provided.", OPT_URI, flagName),
          String.format("If the --%s value is valid, please omit --%s.", OPT_URI, flagName),
          String.format("If all connection parameters are provided, please omit the --%s", OPT_URI)
        };
    return new MetadataDumperUsageException(String.join(" ", sentences));
  }

  private MetadataDumperUsageException noAssessmentException() {
    String message =
        String.format(
            "The %s connector only supports extraction for Assessment."
                + " Provide the '--%s' flag to use this connector.",
            getName(), ConnectorArguments.OPT_ASSESSMENT);
    return new MetadataDumperUsageException(message);
  }

  private MetadataDumperUsageException unsupportedAssessmentException() {
    String message =
        String.format(
            "The %s connector supports assessment without need for extra flags."
                + " Try running again without the '--%s' flag",
            getName(), ConnectorArguments.OPT_ASSESSMENT);
    return new MetadataDumperUsageException(message);
  }
}
