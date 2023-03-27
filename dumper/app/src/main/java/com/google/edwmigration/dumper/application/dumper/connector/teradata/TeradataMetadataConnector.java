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

import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataUtils.formatQuery;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabasePredicate;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.utils.SqlBuilder;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataMetadataDumpFormat;
import java.util.List;
import java.util.OptionalLong;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;

/** @author miguel */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from Teradata.")
@RespectsArgumentDatabasePredicate
@RespectsArgumentAssessment
public class TeradataMetadataConnector extends AbstractTeradataConnector
    implements MetadataConnector, TeradataMetadataDumpFormat {

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
        "Max number of database rows (rows with DBKind='D') to extract from dbc.DatabasesV table.");

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
    String whereDatabaseNameClause =
        new SqlBuilder()
            .withWhereInVals("\"DatabaseName\"", arguments.getDatabases())
            .toWhereClause();
    String whereDataBaseNameClause =
        new SqlBuilder()
            .withWhereInVals("\"DataBaseName\"", arguments.getDatabases())
            .toWhereClause();
    String whereBDatabaseNameClause =
        new SqlBuilder()
            .withWhereInVals("\"B_DatabaseName\"", arguments.getDatabases())
            .toWhereClause();
    String whereChildDBClause =
        new SqlBuilder().withWhereInVals("\"ChildDB\"", arguments.getDatabases()).toWhereClause();
    String whereParentDBClause =
        new SqlBuilder().withWhereInVals("\"ParentDB\"", arguments.getDatabases()).toWhereClause();

    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    out.add(
        new TeradataJdbcSelectTask(
            VersionFormat.ZIP_ENTRY_NAME,
            TaskCategory.OPTIONAL,
            "SELECT 'teradata' AS dialect, "
                + "\"InfoData\" AS version, "
                + "CURRENT_TIMESTAMP as export_time"
                + " from dbc.dbcinfo where \"InfoKey\" = 'VERSION';"));

    // This is theoretically more reliable than ColumnsV, but if we are bandwidth limited, we should
    // risk taking ColumnsV only.
    // out.add(new JdbcSelectTask(ColumnsFormat.ZIP_ENTRY_NAME, // Was: teradata.columns.csv
    // "SELECT \"DatabaseName\", \"TableName\", \"ColumnId\", \"ColumnName\", \"ColumnType\" FROM
    // DBC.Columns" + whereDatabaseNameClause + " ;"));
    out.add(createTaskForDatabasesV(whereDatabaseNameClause, arguments));
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
            "SELECT %s FROM DBC.TablesV" + whereDataBaseNameClause + " ;"));

    out.add(
        new TeradataJdbcSelectTask(
            TableTextVFormat.ZIP_ENTRY_NAME,
            TaskCategory.REQUIRED, // Documented since v15.00
            "SELECT %s FROM DBC.TableTextV" + whereDataBaseNameClause + " ;"));

    out.add(
        new TeradataJdbcSelectTask(
            IndicesVFormat.ZIP_ENTRY_NAME,
            TaskCategory.OPTIONAL,
            "SELECT %s FROM DBC.IndicesV" + whereDatabaseNameClause + " ;"));

    out.add(
        new TeradataJdbcSelectTask(
            PartitioningConstraintsVFormat.ZIP_ENTRY_NAME,
            TaskCategory.OPTIONAL,
            "SELECT %s FROM DBC.PartitioningConstraintsV" + whereDatabaseNameClause + " ;"));

    out.add(
        new TeradataJdbcSelectTask(
            ColumnsVFormat.ZIP_ENTRY_NAME,
            TaskCategory.REQUIRED,
            "SELECT %s FROM DBC.ColumnsV" + whereDatabaseNameClause + " ;"));

    out.add(
        new TeradataJdbcSelectTask(
            FunctionsVFormat.ZIP_ENTRY_NAME,
            TaskCategory.OPTIONAL,
            "SELECT %s FROM DBC.FunctionsV" + whereDatabaseNameClause + " ;"));

    if (arguments.isAssessment()) {
      out.add(
          new TeradataJdbcSelectTask(
              StatsVFormat.ZIP_ENTRY_NAME,
              TaskCategory.OPTIONAL,
              "SELECT %s FROM DBC.StatsV " + whereDatabaseNameClause + " ;"));

      out.add(createTaskForTableSizeV(whereDataBaseNameClause, arguments));

      out.add(
          new TeradataJdbcSelectTask(
              AllTempTablesVXFormat.ZIP_ENTRY_NAME,
              TaskCategory.OPTIONAL,
              "SELECT %s FROM DBC.AllTempTablesVX" + whereBDatabaseNameClause + " ;"));

      out.add(createTaskForDiskSpaceV(whereDataBaseNameClause, arguments));

      out.add(
          new TeradataJdbcSelectTask(
              RoleMembersVFormat.ZIP_ENTRY_NAME,
              TaskCategory.OPTIONAL,
              "SELECT %s FROM DBC.RoleMembersV ;"));

      out.add(
          new TeradataJdbcSelectTask(
              All_RI_ChildrenVFormat.ZIP_ENTRY_NAME,
              TaskCategory.OPTIONAL,
              "SELECT %s FROM DBC.All_RI_ChildrenV " + whereChildDBClause + " ;"));

      out.add(
          new TeradataJdbcSelectTask(
              All_RI_ParentsVFormat.ZIP_ENTRY_NAME,
              TaskCategory.OPTIONAL,
              "SELECT %s FROM DBC.All_RI_ParentsV " + whereParentDBClause + " ;"));
    }
  }

  private TeradataJdbcSelectTask createTaskForDatabasesV(
      String whereDatabaseNameClause, ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    StringBuilder query = new StringBuilder();
    OptionalLong userRows =
        parseMaxRows(arguments, TeradataMetadataConnectorProperties.DATABASES_V_USERS_MAX_ROWS);
    OptionalLong dbRows =
        parseMaxRows(arguments, TeradataMetadataConnectorProperties.DATABASES_V_DBS_MAX_ROWS);
    query.append("SELECT %s FROM ");
    if (!userRows.isPresent() && !dbRows.isPresent()) {
      query.append(" DBC.DatabasesV ").append(whereDatabaseNameClause);
    } else {
      query.append(" (SELECT * FROM ( ");
      appendSelect(
          query,
          userRows,
          " * FROM DBC.DatabasesV " + concatWhere(whereDatabaseNameClause, " DBKind='U' "),
          " ORDER BY PermSpace DESC ");
      query.append(" ) AS users UNION SELECT * FROM (");
      appendSelect(
          query,
          dbRows,
          " * FROM DBC.DatabasesV " + concatWhere(whereDatabaseNameClause, " DBKind='D' "),
          " ORDER BY PermSpace DESC ");
      query.append(" ) AS dbs) AS t");
    }
    query.append(';');
    return new TeradataJdbcSelectTask(
        DatabasesVFormat.ZIP_ENTRY_NAME, TaskCategory.REQUIRED, formatQuery(query.toString()));
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
    String stringValue = arguments.getDefinition(property);
    if (StringUtils.isEmpty(stringValue)) {
      return OptionalLong.empty();
    }
    long value;
    try {
      value = Long.parseLong(stringValue);
    } catch (NumberFormatException ex) {
      throw new MetadataDumperUsageException(
          String.format(
              "ERROR: Option '%s' accepts only positive integers. Actual: '%s'.",
              property.name, stringValue));
    }
    if (value <= 0) {
      throw new MetadataDumperUsageException(
          String.format(
              "ERROR: Option '%s' accepts only positive integers. Actual: '%s'.",
              property.name, stringValue));
    }
    return OptionalLong.of(value);
  }

  private static String concatWhere(String whereClause, String condition) {
    StringBuilder result = new StringBuilder();
    if (whereClause.isEmpty()) {
      result.append(" WHERE ");
    } else {
      result.append(whereClause);
    }
    result.append(condition);
    return result.toString();
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
