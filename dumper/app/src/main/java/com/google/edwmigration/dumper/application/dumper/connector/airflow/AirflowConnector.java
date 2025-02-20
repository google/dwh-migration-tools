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
package com.google.edwmigration.dumper.application.dumper.connector.airflow;

import static com.google.edwmigration.dumper.application.dumper.connector.airflow.AirflowDatabaseDriverClasses.jdbcPrefixForClassName;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriverRequired;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPassword;
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
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.sql.Driver;
import java.time.Clock;
import java.util.List;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(Connector.class)
@Description("Dumps DAGs metadata from Airflow.")
@RespectsInput(
    order = 100,
    arg = ConnectorArguments.OPT_HOST,
    description = "Airflow database host.")
@RespectsInput(
    order = 200,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the database.")
@RespectsInput(
    order = 250,
    arg = ConnectorArguments.OPT_SCHEMA,
    defaultValue = "airflow_db",
    description = "Airflow database schema name")
@RespectsArgumentDriverRequired
@RespectsArgumentUser
@RespectsArgumentPassword
public class AirflowConnector extends AbstractJdbcConnector implements MetadataConnector {

  private static final Logger LOG = LoggerFactory.getLogger(AirflowConnector.class);

  private static final String FORMAT_NAME = "airflow.dump.zip";

  private final ImmutableList<String> driverClasses =
      ImmutableList.of(
          // the order is important! The first class found will be used as a jdbc connection.
          AirflowDatabaseDriverClasses.MARIADB.getDriverClassName(),
          AirflowDatabaseDriverClasses.MYSQL.getDriverClassName(),
          AirflowDatabaseDriverClasses.MYSQL_OLD.getDriverClassName(),
          AirflowDatabaseDriverClasses.POSTGRESQL.getDriverClassName());

  public AirflowConnector() {
    super("airflow");
  }

  @Nonnull
  @Override
  public String getDefaultFileName(boolean isAssessment, Clock clock) {
    return ArchiveNameUtil.getFileNameWithTimestamp(getName(), clock);
  }

  @Override
  public void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    // Airflow v1.5.0
    addFullTable(out, "dag.csv", "select * from dag;");
    // Airflow v1.6.0
    addFullTable(out, "dag_run.csv", "select * from dag_run;");

    // Airflow v1.10.7
    // analog of DAG's python definition in json
    addFullTable(out, "serialized_dag.csv", "select * from serialized_dag;", TaskCategory.OPTIONAL);
    // Airflow v1.10.10
    addFullTable(out, "dag_code.csv", "select * from dag_code;", TaskCategory.OPTIONAL);
  }

  private static void addFullTable(List<? super Task<?>> out, String filename, String sql) {
    addFullTable(out, filename, sql, TaskCategory.REQUIRED);
  }

  private static void addFullTable(
      List<? super Task<?>> out, String filename, String sql, TaskCategory taskCategory) {
    out.add(new JdbcSelectTask(filename, sql, taskCategory));
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    // todo support jdbc string in --url
    Preconditions.checkNotNull(arguments.getHost(), "Database host must be provided.");
    Preconditions.checkNotNull(arguments.getPort(), "Database port must be provided.");
    Preconditions.checkState(
        arguments.getDriverPaths() != null && !arguments.getDriverPaths().isEmpty(),
        "Path to jdbc driver must be provided");

    // todo fix warning logs, because no drivers is expected case
    Driver driver = loadFirstAvailableDriver(arguments.getDriverPaths());
    String host = arguments.getHost();
    int port = arguments.getPort();
    String schema = arguments.getSchema();

    String jdbcString =
        jdbcPrefixForClassName(driver.getClass().getName()) + host + ":" + port + "/" + schema;
    LOG.info("Connecting to jdbc string [{}]...", jdbcString);

    DataSource dataSource = newSimpleDataSource(driver, jdbcString, arguments);
    return JdbcHandle.newPooledJdbcHandle(dataSource, 1);
  }

  private Driver loadFirstAvailableDriver(List<String> driverPaths) throws Exception {
    return newDriver(driverPaths, driverClasses.toArray(driverClasses.toArray(new String[0])));
  }
}
