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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.MetadataView.TABLE_STORAGE_METRICS;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeInput.USAGE_THEN_SCHEMA_SOURCE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabaseForConnection;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabasePredicate;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakePlanner.AssessmentQuery;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle.WriteMode;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask.TaskOptions;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * A connector to Snowflake databases.
 *
 * @author matt
 */
@AutoService(Connector.class)
@RespectsArgumentAssessment
@RespectsArgumentDatabaseForConnection
@RespectsArgumentDatabasePredicate
public class SnowflakeMetadataConnector extends AbstractSnowflakeConnector
    implements MetadataConnector, SnowflakeMetadataDumpFormat {

  private static final String ACCOUNT_USAGE_SCHEMA_NAME = "SNOWFLAKE.ACCOUNT_USAGE";
  private static final String ACCOUNT_USAGE_WHERE_CONDITION = "DELETED IS NULL";
  private static final String EMPTY_WHERE_CONDITION = "";
  private static final String ACCOUNT_USAGE_SIMPLE_FILE = "account-usage-simple.sql";
  private static final String ACCOUNT_USAGE_COMPLEX_FILE = "account-usage-complex.sql";
  private static final String SHOW_BASED_FILE = "show-based.sql";
  private static final String SNOWFLAKE_FEATURES_PREFIX = "snowflake-features/";

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
  public String getDescription() {
    return "Dumps metadata from Snowflake.";
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

  @Override
  protected void validateForConnector(@Nonnull ConnectorArguments arguments) {
    boolean hasDatabases = !arguments.getDatabases().isEmpty();
    if (arguments.isAssessment() && hasDatabases) {
      throw SnowflakeUsageException.unsupportedFilter();
    }
  }

  private void addSqlTasksWithInfoSchemaFallback(
      @Nonnull List<? super Task<?>> out,
      @Nonnull Class<? extends Enum<?>> header,
      @Nonnull String format,
      @Nonnull String informationSchemaFileName,
      @Nonnull String informationSchemaName,
      @Nonnull String accountUsageFileName,
      @Nonnull String accountUsageSchemaName,
      @Nonnull String accountUsageWhereCondition,
      boolean isAssessment,
      @Nonnull String databaseFilter) {
    AbstractJdbcTask<Summary> schemaTask =
        SnowflakeTaskUtil.withFilter(
            format,
            informationSchemaName,
            informationSchemaFileName,
            ImmutableList.of(databaseFilter),
            header);
    AbstractJdbcTask<Summary> usageTask =
        SnowflakeTaskUtil.withFilter(
            format,
            accountUsageSchemaName,
            accountUsageFileName,
            ImmutableList.of(accountUsageWhereCondition, databaseFilter),
            header);
    if (isAssessment) {
      out.add(usageTask);
    } else {
      out.addAll(inputSource.sqlTasks(schemaTask, usageTask));
    }
  }

  @Override
  public final void addTasksTo(
      @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    out.add(SnowflakeYamlSummaryTask.create(FORMAT_NAME, arguments));

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
        ACCOUNT_USAGE_SCHEMA_NAME,
        ACCOUNT_USAGE_WHERE_CONDITION,
        isAssessment,
        getInformationSchemaWhereCondition("database_name", arguments.getDatabases()));

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
        ACCOUNT_USAGE_SCHEMA_NAME,
        ACCOUNT_USAGE_WHERE_CONDITION,
        isAssessment,
        getInformationSchemaWhereCondition("catalog_name", arguments.getDatabases()));

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
        ACCOUNT_USAGE_SCHEMA_NAME,
        ACCOUNT_USAGE_WHERE_CONDITION,
        isAssessment,
        getInformationSchemaWhereCondition(
            "table_catalog", arguments.getDatabases())); // Painfully slow.

    addSqlTasksWithInfoSchemaFallback(
        out,
        ColumnsFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT table_catalog, table_schema, table_name, ordinal_position, column_name,"
                + " data_type, is_nullable, column_default, character_maximum_length,"
                + " numeric_precision, numeric_scale, datetime_precision, comment FROM %1$s.COLUMNS%2$s",
            MetadataView.COLUMNS),
        ColumnsFormat.IS_ZIP_ENTRY_NAME,
        IS,
        ColumnsFormat.AU_ZIP_ENTRY_NAME,
        ACCOUNT_USAGE_SCHEMA_NAME,
        ACCOUNT_USAGE_WHERE_CONDITION,
        isAssessment,
        getInformationSchemaWhereCondition(
            "table_catalog", arguments.getDatabases())); // Very fast.

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
        ACCOUNT_USAGE_SCHEMA_NAME,
        ACCOUNT_USAGE_WHERE_CONDITION,
        isAssessment,
        getInformationSchemaWhereCondition("table_catalog", arguments.getDatabases()));

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
        ACCOUNT_USAGE_SCHEMA_NAME,
        ACCOUNT_USAGE_WHERE_CONDITION,
        isAssessment,
        getInformationSchemaWhereCondition("function_catalog", arguments.getDatabases()));

    stream(FeaturesQueryPath.values())
        .forEach(
            path -> {
              TaskOptions taskOptions =
                  path.value.contains(ACCOUNT_USAGE_SIMPLE_FILE)
                      ? TaskOptions.DEFAULT
                      : TaskOptions.DEFAULT.withWriteMode(WriteMode.APPEND_EXISTING);
              out.add(
                  new JdbcSelectTask(
                          FeaturesFormat.IS_ZIP_ENTRY_NAME,
                          loadFile(path.value),
                          TaskCategory.OPTIONAL, // TODO: Change to REQUIRED after implementation
                          taskOptions)
                      .withHeaderClass(FeaturesFormat.Header.class));
            });

    if (isAssessment) {
      for (AssessmentQuery item : planner.generateAssessmentQueries()) {
        String query = queryForAssessment(item, arguments);
        Task<?> task =
            new JdbcSelectTask(item.zipEntryName, query, TaskCategory.REQUIRED, TaskOptions.DEFAULT)
                .withHeaderTransformer(item.transformer());
        out.add(task);
      }
      return;
    }
    ImmutableList<String> databases = arguments.getDatabases();

    if (databases.isEmpty()) {
      AssessmentQuery query = SnowflakePlanner.SHOW_EXTERNAL_TABLES;
      Task<?> task = convertAssessmentQuery(query, arguments, TaskOptions.DEFAULT);
      out.add(task);
      return;
    }

    TaskOptions taskOptions = TaskOptions.DEFAULT;

    for (String item : databases) {
      String quotedName = databaseNameQuoted(item);
      AssessmentQuery baseQuery = SnowflakePlanner.SHOW_EXTERNAL_TABLES;

      String formatString = String.format("%s IN DATABASE %s", baseQuery.formatString, quotedName);
      AssessmentQuery query = baseQuery.withFormatString(formatString);
      Task<?> task = convertAssessmentQuery(query, arguments, taskOptions);
      out.add(task);
      // Next tasks will append to the same file.
      taskOptions = taskOptions.withWriteMode(WriteMode.APPEND_EXISTING);
    }
  }

  enum FeaturesQueryPath {
    SIMPLE(SNOWFLAKE_FEATURES_PREFIX + ACCOUNT_USAGE_SIMPLE_FILE),
    COMPLEX(SNOWFLAKE_FEATURES_PREFIX + ACCOUNT_USAGE_COMPLEX_FILE),
    SHOW_BASED(SNOWFLAKE_FEATURES_PREFIX + SHOW_BASED_FILE);

    final String value;

    FeaturesQueryPath(String value) {
      this.value = value;
    }
  }

  private String queryForAssessment(AssessmentQuery item, ConnectorArguments arguments) {
    MetadataView view = TABLE_STORAGE_METRICS;
    String schema = ACCOUNT_USAGE_SCHEMA_NAME;
    if (!item.needsOverride) {
      return item.substitute(schema, "");
    }

    ConnectorProperty propertyQuery = PropertyAction.QUERY.toProperty(view);
    String overrideQuery = arguments.getDefinition(propertyQuery);
    if (overrideQuery != null) {
      return String.format(overrideQuery, schema, "");
    }

    ConnectorProperty propertyWhere = PropertyAction.WHERE.toProperty(view);
    String overrideWhere = arguments.getDefinition(propertyWhere);
    if (overrideWhere != null) {
      return item.substitute(schema, overrideWhere);
    }

    String whereCondition =
        " WHERE deleted = FALSE AND schema_dropped IS NULL AND table_dropped IS NULL";
    return item.substitute(schema, whereCondition);
  }

  private Task<?> convertAssessmentQuery(
      @Nonnull AssessmentQuery item,
      @Nonnull ConnectorArguments arguments,
      @Nonnull TaskOptions taskOptions) {
    String formatString = overrideFormatString(item, arguments);
    String query = String.format(formatString, ACCOUNT_USAGE_SCHEMA_NAME, EMPTY_WHERE_CONDITION);
    String zipName = item.zipEntryName;
    return new JdbcSelectTask(zipName, query, TaskCategory.REQUIRED, taskOptions)
        .withHeaderTransformer(item.transformer());
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
      // Partially format the SQL template by re-introducing the first format specifier.
      return String.format(defaultSql, "%1$s", " WHERE " + overrideWhere);
    }

    return defaultSql;
  }

  private static String getInformationSchemaWhereCondition(
      @Nonnull String databaseNameColumn, @Nonnull ImmutableList<String> databaseNames) {
    if (databaseNames.isEmpty()) {
      return EMPTY_WHERE_CONDITION;
    }
    String quotedNames =
        databaseNames.stream()
            .map(SnowflakeMetadataConnector::databaseNameStringLiteral)
            .collect(Collectors.joining(", "));

    return String.format("%s IN (%s)", databaseNameColumn, quotedNames);
  }

  @VisibleForTesting
  public static String databaseNameStringLiteral(@Nonnull String databaseName) {
    if (databaseName.startsWith("\"") && databaseName.endsWith("\"")) {
      // This is a quoted identifier, it should be matched case-sensitively
      databaseName = databaseName.substring(1, databaseName.length() - 1);
    } else {
      // Unquoted identifiers are stored uppercase, single quotes need to be escaped.
      databaseName = databaseName.toUpperCase();
    }
    databaseName = databaseName.replace("'", "''");
    return String.format("'%s'", databaseName);
  }

  @VisibleForTesting
  public static String databaseNameQuoted(@Nonnull String databaseName) {
    if (databaseName.startsWith("\"") && databaseName.endsWith("\"")) {
      // This is a quoted identifier, it should be matched case-sensitively
      databaseName = databaseName.substring(1, databaseName.length() - 1);
    } else {
      // Unquoted identifiers are stored uppercase, single quotes need to be escaped.
      databaseName = databaseName.toUpperCase();
    }
    databaseName = databaseName.replace("\"", "\"\"");
    return String.format("\"%s\"", databaseName);
  }

  private static String loadFile(String path) {
    try {
      URL queryUrl = Resources.getResource(path);
      return Resources.toString(queryUrl, UTF_8);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format("An invalid file was provided: '%s'.", path), e);
    }
  }
}
