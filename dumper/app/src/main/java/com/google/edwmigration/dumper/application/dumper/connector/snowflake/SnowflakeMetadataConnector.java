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

import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.FeaturesQueryPath.COMPLEX;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.FeaturesQueryPath.SHOW_BASED;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.FeaturesQueryPath.SIMPLE;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.MetadataView.TABLE_STORAGE_METRICS;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeInput.USAGE_THEN_SCHEMA_SOURCE;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabaseForConnection;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabasePredicate;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentSchemaPredicate;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakePlanner.AssessmentQuery;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle.WriteMode;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask.TaskOptions;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connector to Snowflake databases.
 *
 * @author matt
 */
@AutoService(Connector.class)
@RespectsArgumentAssessment
@RespectsArgumentDatabaseForConnection
@RespectsArgumentDatabasePredicate
@RespectsArgumentSchemaPredicate
public class SnowflakeMetadataConnector extends AbstractSnowflakeConnector
    implements MetadataConnector, SnowflakeMetadataDumpFormat {

  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeMetadataConnector.class);
  private static final String ACCOUNT_USAGE_SCHEMA_NAME = "SNOWFLAKE.ACCOUNT_USAGE";
  private static final String ACCOUNT_USAGE_WHERE_CONDITION = "DELETED IS NULL";
  private static final String EMPTY_WHERE_CONDITION = "";

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
    boolean hasSchemata = !arguments.getSchemata().isEmpty();
    if (arguments.isAssessment() && (hasDatabases || hasSchemata)) {
      throw SnowflakeUsageException.unsupportedFilter();
    }
  }

  private void addSqlTasksWithInfoSchemaFallback(
      @Nonnull List<? super Task<?>> out,
      @Nonnull Class<? extends Enum<?>> header,
      @Nonnull String format,
      @Nonnull String informationSchemaFileName,
      @Nonnull String accountUsageFileName,
      @Nonnull String accountUsageWhereCondition,
      @Nonnull ConnectorArguments arguments,
      @Nonnull String databaseFilterColumnName,
      @Nonnull String schemaFilterColumnName) {
    ImmutableList<String> databases = arguments.getDatabases();
    ImmutableList<String> schemata = arguments.getSchemata();
    boolean isAssessment = arguments.isAssessment();
    String globalDatabaseFilter =
        getInformationSchemaWhereCondition(databaseFilterColumnName, databases);
    String globalSchemaFilter =
        schemaFilterColumnName.equals(EMPTY_WHERE_CONDITION)
            ? EMPTY_WHERE_CONDITION
            : getInformationSchemaWhereCondition(schemaFilterColumnName, schemata);
    String cloneDatabaseFilter =
        arguments.isIgnoreCloneOnlyDatabase()
            ? String.format(
                "NVL(%s, '') NOT IN (SELECT table_catalog FROM %s.TABLE_STORAGE_METRICS WHERE"
                    + " deleted = FALSE AND schema_dropped IS NULL AND table_dropped IS NULL AND"
                    + " table_catalog IS NOT NULL GROUP BY table_catalog HAVING COUNT(CASE WHEN id"
                    + " = clone_group_id THEN 1 END) = 0)",
                databaseFilterColumnName, ACCOUNT_USAGE_SCHEMA_NAME)
            : EMPTY_WHERE_CONDITION;
    AbstractJdbcTask<Summary> usageTask =
        SnowflakeTaskUtil.createJdbcSelectTask(
            format,
            ACCOUNT_USAGE_SCHEMA_NAME,
            accountUsageFileName,
            ImmutableList.of(
                accountUsageWhereCondition,
                globalDatabaseFilter,
                globalSchemaFilter,
                cloneDatabaseFilter),
            header);
    if (isAssessment) {
      out.add(usageTask);
      return;
    }

    if (databases.isEmpty()) {
      AbstractJdbcTask<Summary> schemaTask =
          SnowflakeTaskUtil.createJdbcSelectTask(
              format,
              "INFORMATION_SCHEMA",
              informationSchemaFileName,
              ImmutableList.of(globalSchemaFilter),
              header);
      out.addAll(inputSource.sqlTasks(schemaTask, usageTask));
      return;
    }

    List<Task<?>> tasks = new ArrayList<>();
    if (inputSource == SnowflakeInput.USAGE_ONLY_SOURCE
        || inputSource == SnowflakeInput.USAGE_THEN_SCHEMA_SOURCE) {
      tasks.add(usageTask);
    }
    if (inputSource == SnowflakeInput.SCHEMA_ONLY_SOURCE
        || inputSource == SnowflakeInput.USAGE_THEN_SCHEMA_SOURCE) {
      // INFORMATION_SCHEMA is database-scoped. To fetch metadata from multiple databases,
      // we must query each database's INFORMATION_SCHEMA individually (e.g.,
      // db.INFORMATION_SCHEMA.TABLES).
      // Prefixing the database name also helps the query optimizer scope the metadata scan,
      // avoiding performance issues that occur when querying INFORMATION_SCHEMA without a database
      // scope.
      //
      // The first task overwrites the output file; subsequent tasks append to it.
      TaskOptions taskOptions = TaskOptions.DEFAULT;
      for (String database : databases) {
        String schemaPrefix = sanitizeDatabaseName(database) + ".INFORMATION_SCHEMA";
        AbstractJdbcTask<Summary> schemaTask =
            SnowflakeTaskUtil.createJdbcSelectTask(
                format,
                schemaPrefix,
                informationSchemaFileName,
                ImmutableList.of(globalSchemaFilter),
                header,
                taskOptions);
        if (inputSource == SnowflakeInput.USAGE_THEN_SCHEMA_SOURCE) {
          schemaTask.onlyIfFailed(usageTask);
        }
        tasks.add(schemaTask);
        taskOptions = taskOptions.withWriteMode(WriteMode.APPEND_EXISTING);
      }
    }
    out.addAll(tasks);
  }

  @Override
  public final void addTasksTo(
      @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    if (arguments.getDatabases().isEmpty() && !arguments.isIgnoreCloneOnlyDatabase()) {
      LOG.warn(
          "No specific database filter (--database) or clone suppression flag"
              + " (--ignore-clone-only-database) was provided. If your Snowflake account contains"
              + " zero-copy cloned databases or daily snapshots, consider running with"
              + " '--ignore-clone-only-database' to exclude clone-only databases and prevent"
              + " excessively large metadata extracts.");
    }

    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    out.add(SnowflakeYamlSummaryTask.create(FORMAT_NAME, arguments));
    if (!arguments.isIgnoreCloneOnlyDatabase()) {
      out.add(new CheckClonedDatabasesTask());
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
        DatabasesFormat.AU_ZIP_ENTRY_NAME,
        ACCOUNT_USAGE_WHERE_CONDITION,
        arguments,
        "database_name",
        EMPTY_WHERE_CONDITION); // Changed to EMPTY_WHERE_CONDITION

    addSqlTasksWithInfoSchemaFallback(
        out,
        SchemataFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT catalog_name, schema_name FROM %1$s.SCHEMATA%2$s",
            MetadataView.SCHEMATA),
        SchemataFormat.IS_ZIP_ENTRY_NAME,
        SchemataFormat.AU_ZIP_ENTRY_NAME,
        ACCOUNT_USAGE_WHERE_CONDITION,
        arguments,
        "catalog_name",
        "schema_name");

    addSqlTasksWithInfoSchemaFallback(
        out,
        TablesFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT table_catalog, table_schema, table_name, table_type, row_count, bytes,"
                + " clustering_key FROM %1$s.TABLES%2$s",
            MetadataView.TABLES),
        TablesFormat.IS_ZIP_ENTRY_NAME,
        TablesFormat.AU_ZIP_ENTRY_NAME,
        ACCOUNT_USAGE_WHERE_CONDITION,
        arguments,
        "table_catalog",
        "table_schema");

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
        ColumnsFormat.AU_ZIP_ENTRY_NAME,
        ACCOUNT_USAGE_WHERE_CONDITION,
        arguments,
        "table_catalog",
        "table_schema");

    addSqlTasksWithInfoSchemaFallback(
        out,
        ViewsFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT table_catalog, table_schema, table_name, view_definition FROM %1$s.VIEWS%2$s",
            MetadataView.VIEWS),
        ViewsFormat.IS_ZIP_ENTRY_NAME,
        ViewsFormat.AU_ZIP_ENTRY_NAME,
        ACCOUNT_USAGE_WHERE_CONDITION,
        arguments,
        "table_catalog",
        "table_schema");

    addSqlTasksWithInfoSchemaFallback(
        out,
        FunctionsFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT function_schema, function_name, data_type, argument_signature FROM"
                + " %1$s.FUNCTIONS%2$s",
            MetadataView.FUNCTIONS),
        FunctionsFormat.IS_ZIP_ENTRY_NAME,
        FunctionsFormat.AU_ZIP_ENTRY_NAME,
        ACCOUNT_USAGE_WHERE_CONDITION,
        arguments,
        "function_catalog",
        "function_schema");

    if (isAssessment) {
      out.addAll(featuresTasks());

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
    List<String> schemata = arguments.getSchemata();
    TaskOptions taskOptions = TaskOptions.DEFAULT;
    if (databases.isEmpty()) {
      AssessmentQuery query = SnowflakePlanner.SHOW_EXTERNAL_TABLES;
      Task<?> task = convertAssessmentQuery(query, arguments, taskOptions);
      if (!schemata.isEmpty() && task instanceof AbstractJdbcTask) {
        ((AbstractJdbcTask<?>) task).withPredicate(createSchemaPredicate("schema_name", schemata));
      }
      out.add(task);
    } else {
      for (String database : databases) {
        String quotedName = identifierNameQuoted(database);
        AssessmentQuery query = planner.externalTablesInDatabase(quotedName);
        AbstractJdbcTask<?> task =
            new JdbcSelectTask(
                    query.zipEntryName, query.formatString, TaskCategory.REQUIRED, taskOptions)
                .withHeaderTransformer(query.transformer());
        if (!schemata.isEmpty()) {
          task.withPredicate(createSchemaPredicate("schema_name", schemata));
        }
        out.add(task);
        taskOptions = taskOptions.withWriteMode(WriteMode.APPEND_EXISTING);
      }
    }
  }

  private static ImmutableList<AbstractJdbcTask<Summary>> featuresTasks() {
    ImmutableList<FeaturesQueryPath> paths = ImmutableList.of(SIMPLE, COMPLEX, SHOW_BASED);
    ImmutableList.Builder<AbstractJdbcTask<Summary>> builder = ImmutableList.builder();
    for (FeaturesQueryPath item : paths) {
      JdbcSelectTask task =
          new JdbcSelectTask(
              "features.csv", item.loadFile(), TaskCategory.OPTIONAL, item.taskOptions());
      builder.add(task.withHeaderClass(FeaturesFormat.Header.class));
    }
    return builder.build();
  }

  // INFORMATION_SCHEMA queries must be qualified with a database
  // name or that a "USE DATABASE" command has previously been run
  // in the same session. Qualify the name to avoid this dependency.
  @Nonnull
  private static String getQualifierPrefix(@Nonnull ConnectorArguments arguments) {
    String informationSchema = "INFORMATION_SCHEMA";
    String databaseName = arguments.getDatabaseSingleName();
    if (databaseName == null) {
      return informationSchema;
    } else {
      return sanitizeDatabaseName(databaseName) + "." + informationSchema;
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
      @Nonnull String columnName, @Nonnull ImmutableList<String> objectNames) {
    if (objectNames.isEmpty()) {
      return EMPTY_WHERE_CONDITION;
    }
    String quotedNames =
        objectNames.stream()
            .map(SnowflakeMetadataConnector::identifierNameStringLiteral)
            .collect(Collectors.joining(", "));

    return String.format("%s IN (%s)", columnName, quotedNames);
  }

  @VisibleForTesting
  public static String identifierNameStringLiteral(@Nonnull String identifierName) {
    String rawIdentifierString = identifierNameStringRaw(identifierName);
    String escapedIdentifierSqlLiteral = rawIdentifierString.replace("'", "''");
    return String.format("'%s'", escapedIdentifierSqlLiteral);
  }

  @VisibleForTesting
  public static String identifierNameQuoted(@Nonnull String identifierName) {
    String rawIdentifierString = identifierNameStringRaw(identifierName);
    String escapedIdentifierName = rawIdentifierString.replace("\"", "\"\"");
    return String.format("\"%s\"", escapedIdentifierName);
  }

  @Nonnull
  public static Predicate<ResultSet> createSchemaPredicate(
      @Nonnull String schemaColumnName, @Nonnull List<String> schemata) {
    if (schemata.isEmpty()) {
      return Predicates.alwaysTrue();
    }
    Predicate<String> schemaPredicate = createSchemaPredicate(schemata);
    return new Predicate<ResultSet>() {
      private int columnIndex = -1;

      @Override
      public boolean apply(ResultSet resultSet) {
        try {
          if (columnIndex == -1) {
            columnIndex = findColumnIndex(resultSet, schemaColumnName);
          }
          if (columnIndex > 0) {
            String schemaValue = resultSet.getString(columnIndex);
            return schemaPredicate.apply(schemaValue);
          }
          return true;
        } catch (SQLException e) {
          return true;
        }
      }
    };
  }

  @Nonnull
  public static Predicate<String> createSchemaPredicate(@Nonnull List<String> schemata) {
    if (schemata.isEmpty()) {
      return Predicates.alwaysTrue();
    }
    Set<String> set =
        schemata.stream()
            .map(SnowflakeMetadataConnector::identifierNameStringRaw)
            .collect(Collectors.toSet());
    return input -> input != null && (set.contains(input));
  }

  private static int findColumnIndex(ResultSet resultSet, String columnName) throws SQLException {
    int count = resultSet.getMetaData().getColumnCount();
    for (int i = 1; i <= count; i++) {
      if (columnName.equalsIgnoreCase(resultSet.getMetaData().getColumnLabel(i))
          || columnName.equalsIgnoreCase(resultSet.getMetaData().getColumnName(i))) {
        return i;
      }
    }
    return -1;
  }

  private static String identifierNameStringRaw(@Nonnull String identifierName) {
    if (identifierName.startsWith("\"") && identifierName.endsWith("\"")) {
      // This is a quoted identifier, it should be matched case-sensitively
      identifierName = identifierName.substring(1, identifierName.length() - 1);
    } else {
      // Unquoted identifiers are stored uppercase, single quotes need to be escaped.
      identifierName = identifierName.toUpperCase();
    }
    return identifierName;
  }

  public enum CheckClonedDatabasesHeader {
    DATABASE_NAME
  }

  private static final class CheckClonedDatabasesTask extends JdbcSelectTask {
    private static final Logger LOG = LoggerFactory.getLogger(CheckClonedDatabasesTask.class);

    private static final String SQL =
        "SELECT table_catalog AS database_name FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS"
            + " WHERE deleted = FALSE AND schema_dropped IS NULL AND table_dropped IS NULL AND"
            + " table_catalog IS NOT NULL GROUP BY table_catalog HAVING COUNT(CASE WHEN id ="
            + " clone_group_id THEN 1 END) = 0";

    private long fullyClonedDbCount = 0;

    CheckClonedDatabasesTask() {
      super("check_cloned_databases.csv", SQL, TaskCategory.OPTIONAL);
      withHeaderClass(CheckClonedDatabasesHeader.class);
    }

    @Override
    protected Summary doInConnection(
        TaskRunContext context, JdbcHandle jdbcHandle, ByteSink sink, Connection connection)
        throws SQLException {
      Summary summary = super.doInConnection(context, jdbcHandle, sink, connection);
      this.fullyClonedDbCount = summary.rowCount();
      if (fullyClonedDbCount > 0) {
        LOG.warn(
            "WARNING: Detected {} fully cloned database(s). Dumping cloned databases can result"
                + " in massive metadata bloat and memory errors during migration. Consider using"
                + " '--ignore-clone-only-database' to automatically exclude databases that consist"
                + " only of cloned tables.",
            fullyClonedDbCount);
      }
      return summary;
    }

    @Override
    public String toString() {
      if (fullyClonedDbCount > 0) {
        return String.format(
            "%s (WARNING: Detected %d fully cloned database(s))",
            getTargetPath(), fullyClonedDbCount);
      }
      return super.toString();
    }
  }
}
