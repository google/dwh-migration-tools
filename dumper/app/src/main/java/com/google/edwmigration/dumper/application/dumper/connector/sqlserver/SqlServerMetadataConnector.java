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
package com.google.edwmigration.dumper.application.dumper.connector.sqlserver;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabaseForConnection;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriver;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentHostUnlessUrl;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPassword;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUri;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUser;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractJdbcConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SqlServerMetadataDumpFormat;
import java.sql.Driver;
import java.util.List;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

/**
 * ./gradlew :compilerworks-application-dumper:installDist && *
 * ./compilerworks-application-dumper/build/install/compilerworks-application-dumper/bin/compilerworks-application-dumper
 * * --connector sqlserver --driver /path/to/mssql-jdbc.jar --host
 * codetestserver.database.windows.net --database CodeTestDataWarehouse --user cw --password
 * password
 *
 * @author swapnil
 */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from SQL Server, Azure, and related platforms.")
@RespectsArgumentHostUnlessUrl
@RespectsInput(
    order = ConnectorArguments.OPT_PORT_ORDER,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the server.",
    required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
    defaultValue = "" + SqlServerMetadataConnector.OPT_PORT_DEFAULT)
@RespectsArgumentUser
@RespectsArgumentPassword
@RespectsArgumentDriver
@RespectsArgumentDatabaseForConnection
@RespectsArgumentUri
public class SqlServerMetadataConnector extends AbstractJdbcConnector
    implements MetadataConnector, SqlServerMetadataDumpFormat {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(SqlServerMetadataConnector.class);

  public static final int OPT_PORT_DEFAULT = 1433;
  private static final String SYSTEM_SCHEMAS =
      "('sys', 'information_schema', 'performance_schema')";

  public SqlServerMetadataConnector() {
    super("sqlserver");
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    out.add(
        new JdbcSelectTask(
            SchemataFormat.ZIP_ENTRY_NAME, "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA"));
    out.add(
        new JdbcSelectTask(TablesFormat.ZIP_ENTRY_NAME, "SELECT * FROM INFORMATION_SCHEMA.TABLES"));
    out.add(
        new JdbcSelectTask(
            ColumnsFormat.ZIP_ENTRY_NAME, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS"));
    out.add(
        new JdbcSelectTask(ViewsFormat.ZIP_ENTRY_NAME, "SELECT * FROM INFORMATION_SCHEMA.VIEWS"));
    out.add(
        new JdbcSelectTask(
            FunctionsFormat.ZIP_ENTRY_NAME, "SELECT * FROM INFORMATION_SCHEMA.ROUTINES"));
  }

  @Override
  public Handle open(ConnectorArguments arguments) throws Exception {
    String url = arguments.getUri();
    if (url == null) {
      StringBuilder buf = new StringBuilder("jdbc:sqlserver://");
      buf.append(arguments.getHost("localhost"));
      buf.append(':').append(arguments.getPort(OPT_PORT_DEFAULT));
      buf.append(";encrypt=true;loginTimeout=30");
      List<String> databases = arguments.getDatabases();
      if (!databases.isEmpty()) buf.append(";database=").append(databases.get(0));
      url = buf.toString();
    }

    // LOG.info("Connecting to URL {}", url);
    Driver driver =
        newDriver(arguments.getDriverPaths(), "com.microsoft.sqlserver.jdbc.SQLServerDriver");
    DataSource dataSource =
        new SimpleDriverDataSource(driver, url, arguments.getUser(), arguments.getPassword());
    return new JdbcHandle(dataSource);
  }
}
