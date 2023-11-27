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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static com.google.edwmigration.dumper.application.dumper.connector.teradata.MetadataQueryGenerator.DBC_INFO_QUERY;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.MetadataQueryGenerator.createSelectForAllTempTablesVX;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.MetadataQueryGenerator.createSimpleSelect;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataUtils.formatQuery;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataUtils.optionalIf;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.identifier;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.in;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.select;
import static java.util.stream.Collectors.joining;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.primitives.Ints;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabasePredicate;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Expression;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.utils.PropertyParser;
import com.google.edwmigration.dumper.application.dumper.utils.SqlBuilder;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataMetadataDumpFormat;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.Nonnull;

/** @author miguel */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from Teradata.")
@RespectsArgumentDatabasePredicate
@RespectsArgumentAssessment
public class TeradataMetadataConnector extends AbstractTeradataConnector
    implements MetadataConnector, TeradataMetadataDumpFormat {

  /** The length of the VARCHAR column {@code TableTextV.RequestText}. */
  private static final int TABLE_TEXT_V_REQUEST_TEXT_LENGTH = 32000;

  private static final Range<Long> MAX_TEXT_LENGTH_RANGE =
      Range.closed(5000L, (long) TABLE_TEXT_V_REQUEST_TEXT_LENGTH);

  public enum TeradataMetadataConnectorProperties implements ConnectorProperty {
    TABLE_SIZE_V_MAX_ROWS(
        "tablesizev.max-rows", "Max number of rows to extract from dbc.TableSizeV table."),
    DISK_SPACE_V_MAX_ROWS(
        "diskspacev.max-rows", "Max number of rows to extract from dbc.DiskSpaceV table."),
    DATABASES_V_USERS_MAX_ROWS(
        "databasesv.users.max-rows",
        "Max number of user rows (rows with DBKind='U') to extract from dbc.DatabasesV table."),
    DATABASES_V_DBS_MAX_ROWS(
        "databasesv.dbs.max-rows",
        "Max number of database rows (rows with DBKind='D') to extract from dbc.DatabasesV table."),
    MAX_TEXT_LENGTH(
        "max-text-length",
        "Max length of the text column when dumping TableTextV view."
            + " Text that is longer than the defined limit will be split into multiple rows."
            + " Example: 10000. Allowed range: "
            + MAX_TEXT_LENGTH_RANGE
            + ".");

    private final String name;
    private final String description;

    TeradataMetadataConnectorProperties(String name, String description) {
      this.name = "teradata.metadata." + name;
      this.description = description;
    }

    @Nonnull
    public String getName() {
      return name;
    }

    @Nonnull
    public String getDescription() {
      return description;
    }
  }

  @Nonnull
  @Override
  public Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return TeradataMetadataConnectorProperties.class;
  }

  public TeradataMetadataConnector() {
    super("teradata");
  }

  /* The sql here are passed to teradata as well as postgresql acting as teradata for testing.
   * QuotedIdentifiers are needed to make the queries work in PG.
   * It looks like quoted teradata identifiers are also case insensitive, so this should be ok. */
  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    Optional<Expression> databaseNameCondition =
        constructDatabaseNameCondition(arguments, "DatabaseName");
    String whereDataBaseNameClause =
        new SqlBuilder()
            .withWhereInVals("\"DataBaseName\"", arguments.getDatabases())
            .toWhereClause();

    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    out.add(
        new TeradataJdbcSelectTask(
            VersionFormat.ZIP_ENTRY_NAME, TaskCategory.OPTIONAL, DBC_INFO_QUERY));

    // This is theoretically more reliable than ColumnsV, but if we are bandwidth limited, we should
    // risk taking ColumnsV only.
    // out.add(new JdbcSelectTask(ColumnsFormat.ZIP_ENTRY_NAME, // Was: teradata.columns.csv
    // "SELECT \"DatabaseName\", \"TableName\", \"ColumnId\", \"ColumnName\", \"ColumnType\" FROM
    // DBC.Columns" + whereDatabaseNameClause + " ;"));
    out.add(createTaskForDatabasesV(arguments, databaseNameCondition));
    // out.add(new TeradataJdbcSelectTask("td.dbc.Tables.others.csv", "SELECT * FROM DBC.Tables
    // WHERE TableKind <> 'F' ORDER BY 1,2,3,4;"));
    // out.add(new TeradataJdbcSelectTask("td.dbc.Tables.functions.csv", "SELECT * FROM DBC.Tables
    // WHERE TableKind = 'F' ORDER BY 1,2,3,4;"));
    // TODO: This contains RequestText for views, which doesn't tell us the "current" database at
    // the point the view was defined.
    // We need the current database because unqualified symbols are resolved in it.
    // TablesV is a view over DBC.TVM, which itself contains a column CreateText, which is a
    // Teradata-generated fully qualified version of the view.
    // We may want/need to dump DBC.TVM to get reliable versions of views.
    out.add(
        new TeradataJdbcSelectTask(
            TablesVFormat.ZIP_ENTRY_NAME,
            TaskCategory.REQUIRED,
            createSimpleSelect("DBC.TablesV", databaseNameCondition)));

    out.add(createTaskForTableTextV(arguments));

    out.add(
        new TeradataJdbcSelectTask(
            IndicesVFormat.ZIP_ENTRY_NAME,
            TaskCategory.OPTIONAL,
            createSimpleSelect("DBC.IndicesV", databaseNameCondition)));

    out.add(
        new TeradataJdbcSelectTask(
            PartitioningConstraintsVFormat.ZIP_ENTRY_NAME,
            TaskCategory.OPTIONAL,
            createSimpleSelect("DBC.PartitioningConstraintsV", databaseNameCondition)));

    out.add(
        new TeradataJdbcSelectTask(
            ColumnsVFormat.ZIP_ENTRY_NAME,
            TaskCategory.REQUIRED,
            createSimpleSelect("DBC.ColumnsV", databaseNameCondition)));

    out.add(
        new TeradataJdbcSelectTask(
            FunctionsVFormat.ZIP_ENTRY_NAME,
            TaskCategory.OPTIONAL,
            createSimpleSelect("DBC.FunctionsV", databaseNameCondition)));

    if (arguments.isAssessment()) {
      out.add(
          new TeradataJdbcSelectTask(
              StatsVFormat.ZIP_ENTRY_NAME,
              TaskCategory.OPTIONAL,
              createSimpleSelect("DBC.StatsV", databaseNameCondition)));

      out.add(createTaskForTableSizeV(whereDataBaseNameClause, arguments));

      out.add(createTaskForAllTempTablesVX(arguments));

      out.add(createTaskForDiskSpaceV(whereDataBaseNameClause, arguments));

      out.add(
          new TeradataJdbcSelectTask(
              RoleMembersVFormat.ZIP_ENTRY_NAME,
              TaskCategory.OPTIONAL,
              select("%s").from("DBC.RoleMembersV").serialize()));

      out.add(
          new TeradataJdbcSelectTask(
              All_RI_ChildrenVFormat.ZIP_ENTRY_NAME,
              TaskCategory.OPTIONAL,
              createSimpleSelect(
                  "DBC.All_RI_ChildrenV", constructDatabaseNameCondition(arguments, "ChildDB"))));

      out.add(
          new TeradataJdbcSelectTask(
              All_RI_ParentsVFormat.ZIP_ENTRY_NAME,
              TaskCategory.OPTIONAL,
              createSimpleSelect(
                  "DBC.All_RI_ParentsV", constructDatabaseNameCondition(arguments, "ParentDB"))));
    }
  }

  private Optional<Expression> constructDatabaseNameCondition(
      ConnectorArguments arguments, String columnName) {
    return optionalIf(
        !arguments.getDatabases().isEmpty(),
        () -> in(identifier(columnName), arguments.getDatabases()));
  }

  private TeradataJdbcSelectTask createTaskForTableTextV(ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    List<String> databases = arguments.getDatabases();
    OptionalLong textMaxLength =
        PropertyParser.parseNumber(
            arguments, TeradataMetadataConnectorProperties.MAX_TEXT_LENGTH, MAX_TEXT_LENGTH_RANGE);
    Optional<String> whereCondition =
        optionalIf(
            !databases.isEmpty(),
            () ->
                "\"DataBaseName\" IN ("
                    + databases.stream()
                        .map(TeradataMetadataConnector::escapeStringLiteral)
                        .collect(joining(","))
                    + ")");

    String query;
    if (textMaxLength.isPresent()) {
      int splitTextColumnMaxLength = Ints.checkedCast(textMaxLength.getAsLong());
      query =
          new SplitTextColumnQueryGenerator(
                  ImmutableList.of("DataBaseName", "TableName", "TableKind"),
                  "RequestText",
                  "LineNo",
                  "DBC.TableTextV",
                  whereCondition,
                  TABLE_TEXT_V_REQUEST_TEXT_LENGTH,
                  splitTextColumnMaxLength)
              .generate();
    } else {
      query =
          "SELECT %s FROM DBC.TableTextV"
              + whereCondition.map(condition -> " WHERE " + condition).orElse("");
    }
    return new TeradataJdbcSelectTask(
        TableTextVFormat.ZIP_ENTRY_NAME, TaskCategory.REQUIRED, query);
  }

  private static String escapeStringLiteral(String s) {
    return "'" + (s.replaceAll("'", "''")) + "'";
  }

  private TeradataJdbcSelectTask createTaskForDatabasesV(
      ConnectorArguments arguments, Optional<Expression> databaseNameCondition)
      throws MetadataDumperUsageException {
    OptionalLong userRows =
        parseMaxRows(arguments, TeradataMetadataConnectorProperties.DATABASES_V_USERS_MAX_ROWS);
    OptionalLong dbRows =
        parseMaxRows(arguments, TeradataMetadataConnectorProperties.DATABASES_V_DBS_MAX_ROWS);
    return new TeradataJdbcSelectTask(
        DatabasesVFormat.ZIP_ENTRY_NAME,
        TaskCategory.REQUIRED,
        MetadataQueryGenerator.createSelectForDatabasesV(userRows, dbRows, databaseNameCondition));
  }

  private TeradataJdbcSelectTask createTaskForAllTempTablesVX(ConnectorArguments arguments) {
    return new TeradataJdbcSelectTask(
        AllTempTablesVXFormat.ZIP_ENTRY_NAME,
        TaskCategory.OPTIONAL,
        createSelectForAllTempTablesVX(arguments.getDatabases()));
  }

  private TeradataJdbcSelectTask createTaskForTableSizeV(
      String whereDataBaseNameClause, ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    StringBuilder query = new StringBuilder();
    // TableSizeV contains a row per each VProc/AMP, so it can grow significantly for large dbs.
    // Hence, we aggregate before dumping.
    // See recommended usage
    // https://docs.teradata.com/r/Teradata-VantageTM-Data-Dictionary/March-2019/Views-Reference/TableSizeV-X/Examples-Using-TableSizeV
    appendSelect(
        query,
        parseMaxRows(arguments, TeradataMetadataConnectorProperties.TABLE_SIZE_V_MAX_ROWS),
        " DataBaseName, AccountName, TableName, SUM(CurrentPerm) CurrentPerm, SUM(PeakPerm) PeakPerm FROM DBC.TableSizeV "
            + whereDataBaseNameClause
            + " GROUP BY 1,2,3 ",
        " ORDER BY 4 DESC ");
    query.append(';');
    return new TeradataJdbcSelectTask(
        TableSizeVFormat.ZIP_ENTRY_NAME, TaskCategory.OPTIONAL, formatQuery(query.toString()));
  }

  private TeradataJdbcSelectTask createTaskForDiskSpaceV(
      String whereDataBaseNameClause, ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    StringBuilder query = new StringBuilder();
    query.append("SELECT %s FROM (");
    appendSelect(
        query,
        parseMaxRows(arguments, TeradataMetadataConnectorProperties.DISK_SPACE_V_MAX_ROWS),
        " * FROM DBC.DiskSpaceV " + whereDataBaseNameClause,
        " ORDER BY CurrentPerm DESC ");
    query.append(") AS t;");
    return new TeradataJdbcSelectTask(
        DiskSpaceVFormat.ZIP_ENTRY_NAME, TaskCategory.OPTIONAL, formatQuery(query.toString()));
  }

  private static OptionalLong parseMaxRows(
      ConnectorArguments arguments, TeradataMetadataConnectorProperties property)
      throws MetadataDumperUsageException {
    return PropertyParser.parseNumber(arguments, property, Range.atLeast(1L));
  }

  private static void appendSelect(
      StringBuilder query, OptionalLong maxRowCountMaybe, String selectBody, String orderBy) {
    query.append(" SELECT ");
    maxRowCountMaybe.ifPresent(
        maxRowCount -> query.append(" TOP ").append(maxRowCount).append(' '));
    query.append(selectBody);
    maxRowCountMaybe.ifPresent(unused -> query.append(orderBy));
  }
}
