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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.ParallelTaskGroup;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftMetadataDumpFormat;
import java.util.List;
import javax.annotation.Nonnull;

/** @author shevek */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from Amazon Redshift.")
@RespectsInput(
    order = ConnectorArguments.OPT_PORT_ORDER,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the server.",
    required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
    defaultValue = "" + RedshiftMetadataConnector.OPT_PORT_DEFAULT)
public class RedshiftMetadataConnector extends AbstractRedshiftConnector
    implements MetadataConnector, RedshiftMetadataDumpFormat {

  private static final String PG_SCHEMAS = "('pg_catalog', 'pg_internal', 'information_schema')";

  public RedshiftMetadataConnector() {
    super("redshift");
  }

  private static void selStar(@Nonnull ParallelTaskGroup out, @Nonnull String name) {
    out.addTask(
        new JdbcSelectTask(name.toLowerCase() + ".csv", "SELECT * FROM " + name.toLowerCase()));
  }

  @Override
  @SuppressWarnings("deprecation")
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) {

    ParallelTaskGroup parallelTask = new ParallelTaskGroup(this.getName());
    out.add(parallelTask);

    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    out.add(new RedshiftEnvironmentYamlTask());

    parallelTask.addTask(
        new JdbcSelectTask(SvvColumnsFormat.ZIP_ENTRY_NAME, "SELECT * FROM SVV_COLUMNS"));
    selStar(parallelTask, "SVV_TABLES");
    selStar(parallelTask, "SVV_TABLE_INFO");

    selStar(parallelTask, "SVV_EXTERNAL_COLUMNS");
    selStar(parallelTask, "SVV_EXTERNAL_DATABASES");
    selStar(parallelTask, "SVV_EXTERNAL_PARTITIONS");
    selStar(parallelTask, "SVV_EXTERNAL_SCHEMAS");
    selStar(parallelTask, "SVV_EXTERNAL_TABLES");

    selStar(parallelTask, "PG_LIBRARY");

    selStar(parallelTask, "pg_database");
    selStar(parallelTask, "pg_namespace");
    selStar(parallelTask, "pg_operator");
    selStar(parallelTask, "pg_tables");

    // \l
    parallelTask.addTask(
        new JdbcSelectTask(
            "database.csv",
            "SELECT d.datid as \"Database_id\", d.datname as \"Name\", pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\", pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\", pg_catalog.array_to_string(d.datacl, ',') AS \"Access_privileges\" FROM pg_catalog.pg_database_info d ORDER BY 1"));
    // \df
    parallelTask.addTask(
        new JdbcSelectTask(
            "functions.csv",
            "SELECT n.nspname as \"Schema\", p.proname as \"Name\","
                + " format_type(p.prorettype, null) as \"Result_data_type\","
                + " oidvectortypes(p.proargtypes) as \"Argument_data_types\","
                + " lang.lanname as \"Language_name\""
                + " FROM pg_catalog.pg_proc p"
                + " LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace"
                + " LEFT JOIN pg_catalog.pg_language lang ON lang.oid = p.prolang"
                + " WHERE pg_catalog.pg_function_is_visible(p.oid) ORDER BY 1, 2, 4"));
    // \dT
    parallelTask.addTask(
        new JdbcSelectTask(
            "types.csv",
            "SELECT n.nspname as \"Schema\", pg_catalog.format_type(t.oid, NULL) AS \"Name\", pg_catalog.obj_description(t.oid, 'pg_type') as \"Description\" FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem ) AND pg_catalog.pg_type_is_visible(t.oid) ORDER BY 1, 2"));
    // \da
    parallelTask.addTask(
        new JdbcSelectTask(
            "aggregates.csv",
            "SELECT n.nspname as \"Schema\", p.proname AS \"Name\", pg_catalog.format_type(p.prorettype, NULL) AS \"Result_data_type\", CASE WHEN p.pronargs = 0 THEN CAST('*' AS pg_catalog.text) ELSE oidvectortypes(p.proargtypes) END AS \"Argument_data_types\", pg_catalog.obj_description(p.oid, 'pg_proc') as \"Description\" FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE p.proisagg AND pg_catalog.pg_function_is_visible(p.oid) ORDER BY 1, 2, 4"));
    // \dC
    parallelTask.addTask(
        new JdbcSelectTask(
            "casts.csv",
            "SELECT pg_catalog.format_type(castsource, NULL) AS \"Source_type\", pg_catalog.format_type(casttarget, NULL) AS \"Target_type\", CASE WHEN castfunc = 0 THEN '(binary coercible)' ELSE p.proname END as \"Function\", CASE WHEN c.castcontext = 'e' THEN 'no' WHEN c.castcontext = 'a' THEN 'in assignment' ELSE 'yes' END as \"Implicit?\"FROM pg_catalog.pg_cast c LEFT JOIN pg_catalog.pg_proc p ON c.castfunc = p.oid LEFT JOIN pg_catalog.pg_type ts ON c.castsource = ts.oid LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid = ts.typnamespace LEFT JOIN pg_catalog.pg_type tt ON c.casttarget = tt.oid LEFT JOIN pg_catalog.pg_namespace nt ON nt.oid = tt.typnamespace WHERE ( (true  AND pg_catalog.pg_type_is_visible(ts.oid)) OR (true  AND pg_catalog.pg_type_is_visible(tt.oid)) ) ORDER BY 1, 2"));
    // Others. TODO: Order?

    parallelTask.addTask(
        new JdbcSelectTask(
            RedshiftMetadataDumpFormat.InformationSchemaColumns.ZIP_ENTRY_NAME_SYSTEM,
            "select * from information_schema.columns where table_schema in " + PG_SCHEMAS));
    parallelTask.addTask(
        new JdbcSelectTask(
            RedshiftMetadataDumpFormat.InformationSchemaColumns.ZIP_ENTRY_NAME,
            "select * from information_schema.columns where table_schema not in " + PG_SCHEMAS));
    parallelTask.addTask(
        new JdbcSelectTask(
            RedshiftMetadataDumpFormat.PgTableDef.ZIP_ENTRY_NAME_SYSTEM,
            "select * from pg_table_def where schemaname in " + PG_SCHEMAS));
    parallelTask.addTask(
        new JdbcSelectTask(
            RedshiftMetadataDumpFormat.PgTableDef.ZIP_ENTRY_NAME,
            "select * from pg_table_def where schemaname not in " + PG_SCHEMAS));
    parallelTask.addTask(
        new JdbcSelectTask(
            RedshiftMetadataDumpFormat.PgViews.ZIP_ENTRY_NAME_SYSTEM,
            "select * from pg_views where schemaname in " + PG_SCHEMAS));
    parallelTask.addTask(
        new JdbcSelectTask(
            RedshiftMetadataDumpFormat.PgViews.ZIP_ENTRY_NAME,
            "select * from pg_views where schemaname not in " + PG_SCHEMAS));
    parallelTask.addTask(
        new JdbcSelectTask(
            RedshiftMetadataDumpFormat.PgUser.ZIP_ENTRY_NAME, "select * from pg_user"));

    if (arguments.isAssessment()) {
      selStar(parallelTask, "STV_MV_INFO");
      selStar(parallelTask, "STV_WLM_SERVICE_CLASS_CONFIG");
      selStar(parallelTask, "STV_WLM_SERVICE_CLASS_STATE");
    }
  }
}
