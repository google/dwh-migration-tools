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
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import java.sql.Driver;
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

  public AbstractOracleConnector(@Nonnull String name) {
    super(name);
  }

  private boolean isOracleSid(ConnectorArguments arguments) throws MetadataDumperUsageException {
    String service = arguments.getOracleServicename();
    String sid = arguments.getOracleSID();
    if (sid != null && service == null) return true;
    else if (sid == null && service != null) return false;
    else
      throw new MetadataDumperUsageException(
          "Provide either -oracle-service or -oracle-sid for oracle dumper");
  }

  //   jdbc:oracle:thin://<host>:<port>/<service>
  //   jdbc:oracle:thin:<host>:<port>:<SID>
  //   jdbc:oracle:thin:<TNSName> (from 10.2.0.1.0)
  @Nonnull
  @Override
  public Handle open(ConnectorArguments arguments) throws Exception {

    String url = arguments.getUri();
    if (url == null) {
      String host = arguments.getHost();
      int port = arguments.getPort(OPT_PORT_DEFAULT);
      if (isOracleSid(arguments)) {
        url = "jdbc:oracle:thin:@" + host + ":" + port + ":" + arguments.getOracleSID();
      } else {
        url = "jdbc:oracle:thin:@//" + host + ":" + port + "/" + arguments.getOracleServicename();
      }
    }
    // LOG.info("URL IS " + url);
    Driver driver = newDriver(arguments.getDriverPaths(), "oracle.jdbc.OracleDriver");
    DataSource dataSource =
        new SimpleDriverDataSource(driver, url, arguments.getUser(), arguments.getPassword());
    return new JdbcHandle(dataSource);
  }
}
