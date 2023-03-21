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
package com.google.edwmigration.dumper.application.dumper.connector.postgresql;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabaseForConnection;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.PostgresqlMetadataDumpFormat;
import java.util.List;

/** @author shevek */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from PostgreSQL.")
@RespectsInput(
    order = ConnectorArguments.OPT_PORT_ORDER,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the server.",
    required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
    defaultValue = "" + PostgresqlMetadataConnector.OPT_PORT_DEFAULT)
@RespectsArgumentDatabaseForConnection
public class PostgresqlMetadataConnector extends AbstractPostgresqlConnector
    implements MetadataConnector, PostgresqlMetadataDumpFormat {

  private static final String PG_SCHEMAS = "('pg_catalog', 'pg_internal', 'information_schema')";

  public PostgresqlMetadataConnector() {
    super("postgresql");
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    // \l
    out.add(
        new JdbcSelectTask(
            "database.csv",
            "SELECT d.datname as \"Name\", pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\", pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\", d.datcollate as \"Collate\", d.datctype as \"Ctype\", pg_catalog.array_to_string(d.datacl, E'\\n') AS \"Access privileges\" FROM pg_catalog.pg_database d ORDER BY 1;"));
    // \df - old postgresql
    out.add(
        new JdbcSelectTask(
            "functions.csv",
            "SELECT n.nspname as Schema, p.proname as Name, pg_catalog.pg_get_function_result(p.oid) as ResultType, pg_catalog.pg_get_function_arguments(p.oid) as ArgumentTypes, CASE WHEN p.proisagg THEN 'agg' WHEN p.proiswindow THEN 'window' WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger' ELSE 'normal' END as Kind FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE pg_catalog.pg_function_is_visible(p.oid) ORDER BY 1, 2, 4;"));
    // \df adapted for pg11: pg_proc.proisagg etc went away.
    // https://www.postgresql.org/docs/11/release-11.html prokind in {w,f,a}
    out.add(
        new JdbcSelectTask(
            "functions-11.csv",
            "SELECT n.nspname as Schema, p.proname as Name, pg_catalog.pg_get_function_result(p.oid) as ResultType, pg_catalog.pg_get_function_arguments(p.oid) as ArgumentTypes, p.prokind as Kind, pg_catalog.obj_description(p.oid, 'pg_proc') as Description FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE pg_catalog.pg_function_is_visible(p.oid) ORDER BY 1, 2, 4;"));
    // \dT
    out.add(
        new JdbcSelectTask(
            "types.csv",
            "SELECT n.nspname as Schema, pg_catalog.format_type(t.oid, NULL) AS Name, pg_catalog.obj_description(t.oid, 'pg_type') as Description FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) AND pg_catalog.pg_type_is_visible(t.oid) ORDER BY 1, 2;"));
    // \da
    out.add(
        new JdbcSelectTask(
            "aggregates.csv",
            "SELECT n.nspname as Schema, p.proname AS Name, pg_catalog.format_type(p.prorettype, NULL) AS ResultType, CASE WHEN p.pronargs = 0 THEN CAST('*' AS pg_catalog.text) ELSE pg_catalog.pg_get_function_arguments(p.oid) END AS ArgumentTypes, pg_catalog.obj_description(p.oid, 'pg_proc') as Description FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE p.proisagg AND pg_catalog.pg_function_is_visible(p.oid) ORDER BY 1, 2, 4;"));
    // \df adapted for pg11: pg_proc.proisagg etc went away.
    // https://www.postgresql.org/docs/11/release-11.html
    out.add(
        new JdbcSelectTask(
            "aggregates-11.csv",
            "SELECT n.nspname as Schema, p.proname AS Name, pg_catalog.format_type(p.prorettype, NULL) AS ResultType, CASE WHEN p.pronargs = 0 THEN CAST('*' AS pg_catalog.text) ELSE pg_catalog.pg_get_function_arguments(p.oid) END AS ArgumentTypes, pg_catalog.obj_description(p.oid, 'pg_proc') as Description FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE p.prokind = 'a' AND pg_catalog.pg_function_is_visible(p.oid) ORDER BY 1, 2, 4;"));

    // \dC
    out.add(
        new JdbcSelectTask(
            "casts.csv",
            "SELECT pg_catalog.format_type(castsource, NULL) AS \"Source type\", pg_catalog.format_type(casttarget, NULL) AS \"Target type\", CASE WHEN castfunc = 0 THEN '(binary coercible)' ELSE p.proname END as \"Function\", CASE WHEN c.castcontext = 'e' THEN 'no' WHEN c.castcontext = 'a' THEN 'in assignment' ELSE 'yes' END as \"Implicit?\"FROM pg_catalog.pg_cast c LEFT JOIN pg_catalog.pg_proc p ON c.castfunc = p.oid LEFT JOIN pg_catalog.pg_type ts ON c.castsource = ts.oid LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid = ts.typnamespace LEFT JOIN pg_catalog.pg_type tt ON c.casttarget = tt.oid LEFT JOIN pg_catalog.pg_namespace nt ON nt.oid = tt.typnamespace WHERE ( (true  AND pg_catalog.pg_type_is_visible(ts.oid)) OR (true  AND pg_catalog.pg_type_is_visible(tt.oid)) ) ORDER BY 1, 2"));
    // Others. TODO: Order?

    out.add(
        new JdbcSelectTask(
            InformationSchemaColumns.ZIP_ENTRY_NAME_SYSTEM,
            "select * from information_schema.columns where table_schema in " + PG_SCHEMAS));
    out.add(
        new JdbcSelectTask(
            InformationSchemaColumns.ZIP_ENTRY_NAME,
            "select * from information_schema.columns where table_schema not in " + PG_SCHEMAS));
    // out.add(new JdbcSelectTask("pg_table_def_generic.csv", "select * from pg_table_def where
    // schemaname in " + PG_SCHEMAS));
    // out.add(new JdbcSelectTask("pg_table_def_private.csv", "select * from pg_table_def where
    // schemaname not in " + PG_SCHEMAS));
    out.add(
        new JdbcSelectTask(
            PgViews.ZIP_ENTRY_NAME_SYSTEM,
            "select * from pg_views where schemaname in " + PG_SCHEMAS));
    out.add(
        new JdbcSelectTask(
            PgViews.ZIP_ENTRY_NAME,
            "select * from pg_views where schemaname not in " + PG_SCHEMAS));
    out.add(new JdbcSelectTask(PgUser.ZIP_ENTRY_NAME, "select * from pg_user"));
  }
}
