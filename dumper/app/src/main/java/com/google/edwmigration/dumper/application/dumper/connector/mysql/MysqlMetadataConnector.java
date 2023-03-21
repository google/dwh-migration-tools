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
package com.google.edwmigration.dumper.application.dumper.connector.mysql;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.MysqlMetadataDumpFormat;
import java.util.List;

/**
 * ./gradlew :compilerworks-application-dumper:installDist &&
 * ./compilerworks-application-dumper/build/install/compilerworks-application-dumper/bin/compilerworks-application-dumper
 * --connector mysql --driver /usr/share/java/mysql-connector-java-8.0.19.jar --user cw --password
 * password
 *
 * @author shevek
 */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from MySQL.")
public class MysqlMetadataConnector extends AbstractMysqlConnector
    implements MetadataConnector, MysqlMetadataDumpFormat {

  // WHERE lower(x) IN ...
  private static final String SYSTEM_SCHEMAS =
      "('mysql', 'sys', 'information_schema', 'performance_schema')";

  public MysqlMetadataConnector() {
    super("mysql");
  }

  @Override
  // TODO: Split system tables into a separate dump, like Postgresql. Figure out a way to do it case
  // insensitively.
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) throws Exception {
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
}
