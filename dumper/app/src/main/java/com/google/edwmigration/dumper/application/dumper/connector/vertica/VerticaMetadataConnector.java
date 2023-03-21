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
package com.google.edwmigration.dumper.application.dumper.connector.vertica;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.VerticaMetadataDumpFormat;
import java.util.List;

/** */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from Vertica.")
public class VerticaMetadataConnector extends AbstractVerticaConnector
    implements MetadataConnector, VerticaMetadataDumpFormat {

  private static final String SYSTEM_SCHEMA = "( 'v_catalog', 'v_monitor' )";

  public VerticaMetadataConnector() {
    super("vertica");
  }

  private void addSelStar(List<? super Task<?>> out, String name, String table) {
    out.add(new JdbcSelectTask(name, "SELECT * FROM V_CATALOG." + table));
  }

  private void addSelStar(
      List<? super Task<?>> out, String name, String table, String schemaField) {
    out.add(
        new JdbcSelectTask(
            name,
            "SELECT * FROM V_CATALOG."
                + table
                + " WHERE "
                + schemaField
                + " NOT IN "
                + SYSTEM_SCHEMA));
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) throws Exception {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    addSelStar(out, AllTablesFormat.ZIP_ENTRY_NAME, "ALL_TABLES", "SCHEMA_NAME");
    addSelStar(out, ColumnsFormat.ZIP_ENTRY_NAME, "COLUMNS", "TABLE_SCHEMA");
    addSelStar(out, ConstraintColumnsFormat.ZIP_ENTRY_NAME, "CONSTRAINT_COLUMNS", "TABLE_SCHEMA");
    addSelStar(out, DatabasesFormat.ZIP_ENTRY_NAME, "DATABASES");
    addSelStar(out, KeywordsFormat.ZIP_ENTRY_NAME, "KEYWORDS");
    addSelStar(out, PrimaryKeysFormat.ZIP_ENTRY_NAME, "PRIMARY_KEYS", "TABLE_SCHEMA");
    addSelStar(out, SchemataFormat.ZIP_ENTRY_NAME, "SCHEMATA");
    addSelStar(out, TableConstraintsFormat.ZIP_ENTRY_NAME, "TABLE_CONSTRAINTS");
    addSelStar(out, TablesFormat.ZIP_ENTRY_NAME, "TABLES", "TABLE_SCHEMA");
    addSelStar(out, TypesFormat.ZIP_ENTRY_NAME, "TYPES");
    addSelStar(out, UserFunctionParametersFormat.ZIP_ENTRY_NAME, "USER_FUNCTION_PARAMETERS");
    addSelStar(out, UserFunctionsFormat.ZIP_ENTRY_NAME, "USER_FUNCTIONS");
    addSelStar(out, UserProceduresFormat.ZIP_ENTRY_NAME, "USER_PROCEDURES");
    addSelStar(out, UsersFormat.ZIP_ENTRY_NAME, "USERS");
    addSelStar(out, ViewColumnsFormat.ZIP_ENTRY_NAME, "VIEW_COLUMNS", "TABLE_SCHEMA");
    addSelStar(out, ViewTablesFormat.ZIP_ENTRY_NAME, "VIEW_TABLES", "TABLE_SCHEMA");
    addSelStar(out, ViewsFormat.ZIP_ENTRY_NAME, "VIEWS", "TABLE_SCHEMA");
  }
}
