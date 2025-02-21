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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.MetadataView.TABLE_STORAGE_METRICS;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeInput.USAGE_THEN_SCHEMA_SOURCE;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakePlanner.AssessmentQuery;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connector to Snowflake databases.
 *
 * @author matt
 */
@AutoService(Connector.class)
@Description("Dumps metadata from Snowflake.")
public class SnowflakeMetadataConnector extends AbstractSnowflakeConnector
    implements MetadataConnector, SnowflakeMetadataDumpFormat {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeMetadataConnector.class);

  private enum PropertyAction {
    QUERY("query", "query"),
    WHERE("where", "where condition to append to query");

    PropertyAction(String value, String description) {
      this.description = description;
      this.value = value;
    }

    final String description;
    final String value;

    ConnectorProperty toProperty(MetadataView metadataView) {
      String name = String.format("snowflake.metadata.%s.%s", metadataView.nameComponent, value);
      String propertyDescription =
          String.format("Custom %s for %s dump.", description, metadataView.description);
      return createProperty(name, propertyDescription);
    }
  }

  private static ConnectorProperty createProperty(String name, String description) {
    return new ConnectorProperty() {

      @Override
      @Nonnull
      public String getDescription() {
        return description;
      }

      @Override
      @Nonnull
      public String getName() {
        return name;
      }
    };
  }

  private final SnowflakeInput inputSource;
  private final SnowflakePlanner planner = new SnowflakePlanner();

  SnowflakeMetadataConnector(@Nonnull String name, @Nonnull SnowflakeInput inputSource) {
    super(name);
    this.inputSource = inputSource;
  }

  public SnowflakeMetadataConnector() {
    this("snowflake", USAGE_THEN_SCHEMA_SOURCE);
  }

  @Override
  @Nonnull
  public Iterable<ConnectorProperty> getPropertyConstants() {
    ImmutableList.Builder<ConnectorProperty> builder = ImmutableList.builder();
    for (MetadataView view : MetadataView.values()) {
      builder.add(PropertyAction.QUERY.toProperty(view));
      builder.add(PropertyAction.WHERE.toProperty(view));
    }
    return builder.build();
  }

  private void addSqlTasksWithInfoSchemaFallback(
      @Nonnull List<? super Task<?>> out,
      @Nonnull Class<? extends Enum<?>> header,
      @Nonnull String format,
      @Nonnull String schemaZip,
      @Nonnull String alteredSchemaView,
      @Nonnull String usageZip,
      @Nonnull String usageView,
      @Nonnull String usageFilter,
      boolean isAssessment) {
    AbstractJdbcTask<Summary> schemaTask =
        SnowflakeTaskUtil.withNoFilter(format, alteredSchemaView, schemaZip, header);
    AbstractJdbcTask<Summary> usageTask =
        SnowflakeTaskUtil.withFilter(format, usageView, usageZip, usageFilter, header);
    if (isAssessment) {
      out.add(usageTask);
    } else {
      ImmutableList<Task<?>> tasks = getSqlTasks(inputSource, schemaTask, usageTask);
      out.addAll(tasks);
    }
  }

  /* For queries to INFORMATION_SCHEMA where there is no ACCOUNT_USAGE equivalent */
  private void addSqlTasksInfoSchemaOnly(
      @Nonnull List<? super Task<?>> out,
      @Nonnull Class<? extends Enum<?>> header,
      @Nonnull String format,
      @Nonnull String schemaZip,
      @Nonnull String alteredSchemaView,
      @Nonnull String filter) {
    AbstractJdbcTask<Summary> usageTask =
        SnowflakeTaskUtil.withFilter(format, alteredSchemaView, schemaZip, filter, header);
    out.add(usageTask);
  }

  @Override
  public final void addTasksTo(
      @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    boolean INJECT_IS_FAULT = arguments.isTestFlag('A');

    // INFORMATION_SCHEMA queries must be qualified with a database
    // name or that a "USE DATABASE" command has previously been run
    // in the same session. Qualify the name to avoid this dependency.
    final String databaseName = arguments.getDatabaseSingleName();
    final String IS;
    if (INJECT_IS_FAULT) {
      IS = "__NONEXISTENT__";
    } else if (databaseName == null) {
      IS = "INFORMATION_SCHEMA";
    } else {
      IS = sanitizeDatabaseName(databaseName) + ".INFORMATION_SCHEMA";
    }
    final String AU = "SNOWFLAKE.ACCOUNT_USAGE";
    final String AU_WHERE = " WHERE DELETED IS NULL";

    boolean isAssessment = arguments.isAssessment();
    addSqlTasksWithInfoSchemaFallback(
        out,
        DatabasesFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT database_name, database_owner FROM %1$s.DATABASES%2$s",
            MetadataView.DATABASES),
        DatabasesFormat.IS_ZIP_ENTRY_NAME,
        IS,
        DatabasesFormat.AU_ZIP_ENTRY_NAME,
        AU,
        AU_WHERE,
        isAssessment);

    addSqlTasksWithInfoSchemaFallback(
        out,
        SchemataFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT catalog_name, schema_name FROM %1$s.SCHEMATA%2$s",
            MetadataView.SCHEMATA),
        SchemataFormat.IS_ZIP_ENTRY_NAME,
        IS,
        SchemataFormat.AU_ZIP_ENTRY_NAME,
        AU,
        AU_WHERE,
        isAssessment);

    addSqlTasksWithInfoSchemaFallback(
        out,
        TablesFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT table_catalog, table_schema, table_name, table_type, row_count, bytes,"
                + " clustering_key FROM %1$s.TABLES%2$s",
            MetadataView.TABLES),
        TablesFormat.IS_ZIP_ENTRY_NAME,
        IS,
        TablesFormat.AU_ZIP_ENTRY_NAME,
        AU,
        AU_WHERE,
        isAssessment); // Painfully slow.

    addSqlTasksInfoSchemaOnly(
        out,
        ExternalTablesFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT table_catalog, table_schema, table_name, location, file_format_name,"
                + " file_format_type FROM %1$s.EXTERNAL_TABLES%2$s",
            MetadataView.TABLES),
        ExternalTablesFormat.ZIP_ENTRY_NAME,
        IS,
        " WHERE table_schema != 'INFORMATION_SCHEMA'");

    addSqlTasksWithInfoSchemaFallback(
        out,
        ColumnsFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT table_catalog, table_schema, table_name, ordinal_position, column_name,"
                + " data_type FROM %1$s.COLUMNS%2$s",
            MetadataView.COLUMNS),
        ColumnsFormat.IS_ZIP_ENTRY_NAME,
        IS,
        ColumnsFormat.AU_ZIP_ENTRY_NAME,
        AU,
        AU_WHERE,
        isAssessment); // Very fast.

    addSqlTasksWithInfoSchemaFallback(
        out,
        ViewsFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT table_catalog, table_schema, table_name, view_definition FROM %1$s.VIEWS%2$s",
            MetadataView.VIEWS),
        ViewsFormat.IS_ZIP_ENTRY_NAME,
        IS,
        ViewsFormat.AU_ZIP_ENTRY_NAME,
        AU,
        AU_WHERE,
        isAssessment);

    addSqlTasksWithInfoSchemaFallback(
        out,
        FunctionsFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT function_schema, function_name, data_type, argument_signature FROM"
                + " %1$s.FUNCTIONS%2$s",
            MetadataView.FUNCTIONS),
        FunctionsFormat.IS_ZIP_ENTRY_NAME,
        IS,
        FunctionsFormat.AU_ZIP_ENTRY_NAME,
        AU,
        AU_WHERE,
        isAssessment);

    if (isAssessment) {
      for (AssessmentQuery item : planner.generateAssessmentQueries()) {
        String formatString = overrideFormatString(item, arguments);
        String query = String.format(formatString, AU, /* an empty WHERE clause */ "");
        String zipName = item.zipEntryName;
        Task<?> task = new JdbcSelectTask(zipName, query).withHeaderTransformer(item.transformer());
        out.add(task);
      }
    }
  }

  private String overrideFormatString(AssessmentQuery query, ConnectorArguments arguments) {
    if (query.needsOverride) {
      return getOverrideableQuery(arguments, query.formatString, TABLE_STORAGE_METRICS);
    } else {
      return query.formatString;
    }
  }

  private String getOverrideableQuery(
      @Nonnull ConnectorArguments arguments,
      @Nonnull String defaultSql,
      @Nonnull MetadataView metadataView) {
    ConnectorProperty propertyQuery = PropertyAction.QUERY.toProperty(metadataView);
    String overrideQuery = arguments.getDefinition(propertyQuery);
    if (overrideQuery != null) {
      return overrideQuery;
    }

    ConnectorProperty propertyWhere = PropertyAction.WHERE.toProperty(metadataView);
    String overrideWhere = arguments.getDefinition(propertyWhere);
    if (overrideWhere != null) {
      return defaultSql + " WHERE " + overrideWhere;
    }

    return defaultSql;
  }
}
