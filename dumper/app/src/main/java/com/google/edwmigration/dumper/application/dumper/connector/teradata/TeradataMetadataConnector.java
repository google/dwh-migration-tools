/*
 * Copyright 2022 Google LLC
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

import com.google.auto.service.AutoService;
import java.util.List;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabasePredicate;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentMetadataStats;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.application.dumper.utils.SqlBuilder;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataMetadataDumpFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author miguel
 */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from Teradata.")
@RespectsArgumentDatabasePredicate
@RespectsArgumentMetadataStats
public class TeradataMetadataConnector extends AbstractTeradataConnector implements MetadataConnector, TeradataMetadataDumpFormat {

    @SuppressWarnings("UnusedVariable")
    private static final Logger LOG = LoggerFactory.getLogger(TeradataMetadataConnector.class);

    public TeradataMetadataConnector() {
        super("teradata");
    }

    /* The sql here are passed to teradata as well as postgresql acting as teradata for testing.
     * QuotedIdentifiers are needed to make the queries work in PG.
     * It looks like quoted teradata identifiers are also case insensitive, so this should be ok. */
    @Override
    public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) {

        String whereDatabaseNameClause = new SqlBuilder().withWhereInVals("\"DatabaseName\"", arguments.getDatabases()).toWhereClause();
        String whereDataBaseNameClause = new SqlBuilder().withWhereInVals("\"DataBaseName\"", arguments.getDatabases()).toWhereClause();
        String whereBDatabaseNameClause = new SqlBuilder().withWhereInVals("\"B_DatabaseName\"", arguments.getDatabases()).toWhereClause();
        String whereChildDBClause = new SqlBuilder().withWhereInVals("\"ChildDB\"", arguments.getDatabases()).toWhereClause();
        String whereParentDBClause = new SqlBuilder().withWhereInVals("\"ParentDB\"", arguments.getDatabases()).toWhereClause();

        out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
        out.add(new FormatTask(FORMAT_NAME));

        out.add(new TeradataJdbcSelectTask(VersionFormat.ZIP_ENTRY_NAME,
                TaskCategory.OPTIONAL,
                "SELECT 'teradata' AS dialect, "
                + "\"InfoData\" AS version, "
                + "CURRENT_TIMESTAMP as export_time"
                + " from dbc.dbcinfo where \"InfoKey\" = 'VERSION';"));

        // This is theoretically more reliable than ColumnsV, but if we are bandwidth limited, we should risk taking ColumnsV only.
        // out.add(new JdbcSelectTask(ColumnsFormat.ZIP_ENTRY_NAME, // Was: teradata.columns.csv
        // "SELECT \"DatabaseName\", \"TableName\", \"ColumnId\", \"ColumnName\", \"ColumnType\" FROM DBC.Columns" + whereDatabaseNameClause + " ;"));
        out.add(new TeradataJdbcSelectTask(DatabasesVFormat.ZIP_ENTRY_NAME,
                TaskCategory.REQUIRED,
                "SELECT %s FROM DBC.DatabasesV" + whereDatabaseNameClause + " ;"));
        //out.add(new TeradataJdbcSelectTask("td.dbc.Tables.others.csv", "SELECT * FROM DBC.Tables WHERE TableKind <> 'F' ORDER BY 1,2,3,4;"));
        //out.add(new TeradataJdbcSelectTask("td.dbc.Tables.functions.csv", "SELECT * FROM DBC.Tables WHERE TableKind = 'F' ORDER BY 1,2,3,4;"));
        // TODO: This contains RequestText for views, which doesn't tell us the "current" database at the point the view was defined.
        // We need the current database because unqualified symbols are resolved in it.
        // TablesV is a view over DBC.TVM, which itself contains a column CreateText, which is a Teradata-generated fully qualified version of the view.
        // We may want/need to dump DBC.TVM to get reliable versions of views.
        out.add(new TeradataJdbcSelectTask(TablesVFormat.ZIP_ENTRY_NAME,
                TaskCategory.REQUIRED,
                "SELECT %s FROM DBC.TablesV" + whereDataBaseNameClause + " ;"));

        out.add(new TeradataJdbcSelectTask(TableTextVFormat.ZIP_ENTRY_NAME,
                TaskCategory.REQUIRED, // Documented since v15.00
                "SELECT %s FROM DBC.TableTextV" + whereDataBaseNameClause + " ;"));

        out.add(new TeradataJdbcSelectTask(IndicesVFormat.ZIP_ENTRY_NAME,
                TaskCategory.OPTIONAL,
                "SELECT %s FROM DBC.IndicesV" + whereDatabaseNameClause + " ;"));

        out.add(new TeradataJdbcSelectTask(PartitioningConstraintsVFormat.ZIP_ENTRY_NAME,
                TaskCategory.OPTIONAL,
                "SELECT %s FROM DBC.PartitioningConstraintsV" + whereDatabaseNameClause + " ;"));

        out.add(new TeradataJdbcSelectTask(ColumnsVFormat.ZIP_ENTRY_NAME,
                TaskCategory.REQUIRED,
                "SELECT %s FROM DBC.ColumnsV" + whereDatabaseNameClause + " ;"));

        out.add(new TeradataJdbcSelectTask(FunctionsVFormat.ZIP_ENTRY_NAME,
                TaskCategory.OPTIONAL,
                "SELECT %s FROM DBC.FunctionsV" + whereDatabaseNameClause + " ;"));

        if (arguments.isMetadataStats()) {
            out.add(new TeradataJdbcSelectTask(StatsVFormat.ZIP_ENTRY_NAME,
                    TaskCategory.OPTIONAL,
                    "SELECT %s FROM DBC.StatsV " + whereDatabaseNameClause + " ;"));

            out.add(new TeradataJdbcSelectTask(TableSizeVFormat.ZIP_ENTRY_NAME,
                    TaskCategory.OPTIONAL,
                    "SELECT %s FROM DBC.TableSizeV" + whereDataBaseNameClause + " ;")); // What's a VProc? (AMP, col 1)

            out.add(new TeradataJdbcSelectTask(AllTempTablesVXFormat.ZIP_ENTRY_NAME,
                TaskCategory.OPTIONAL,
                "SELECT %s FROM DBC.AllTempTablesVX" + whereBDatabaseNameClause + " ;"));

            out.add(new TeradataJdbcSelectTask(DiskSpaceVFormat.ZIP_ENTRY_NAME,
                TaskCategory.OPTIONAL,
                "SELECT %s FROM DBC.DiskSpaceV" + whereDataBaseNameClause + " ;"));

            out.add(new TeradataJdbcSelectTask(RoleMembersVFormat.ZIP_ENTRY_NAME,
                TaskCategory.OPTIONAL,
                "SELECT %s FROM DBC.RoleMembersV ;"));

            out.add(new TeradataJdbcSelectTask(All_RI_ChildrenVFormat.ZIP_ENTRY_NAME,
                TaskCategory.OPTIONAL,
                "SELECT %s FROM DBC.All_RI_ChildrenV " + whereChildDBClause + " ;"));

            out.add(new TeradataJdbcSelectTask(All_RI_ParentsVFormat.ZIP_ENTRY_NAME,
                TaskCategory.OPTIONAL,
                "SELECT %s FROM DBC.All_RI_ParentsV " + whereParentDBClause + " ;"));
        }
    }

}


/*
14:44:43.475 [main] WARN  com.google.edwmigration.dumper.application.dumper.MetadataDumper - Task failed: Write dbc.ColumnsqV.csv from:
SELECT * FROM DBC.ColumnsqV ORDER BY 1,2,3,4;: org.springframework.jdbc.UncategorizedSQLException: StatementCallback; uncategorized SQLException for SQL [SELECT * FROM DBC.ColumnsqV ORDER BY 1,2,3,4;]; SQL state [HY000]; error code [9719]; [Teradata Database] [TeraJDBC 16.20.00.12] [Error 9719] [SQLState HY000] QVCI feature is disabled.; nested exception is java.sql.SQLException: [Teradata Database] [TeraJDBC 16.20.00.12] [Error 9719] [SQLState HY000] QVCI feature is disabled.
org.springframework.jdbc.UncategorizedSQLException: StatementCallback; uncategorized SQLException for SQL [SELECT * FROM DBC.ColumnsqV ORDER BY 1,2,3,4;]; SQL state [HY000]; error code [9719]; [Teradata Database] [TeraJDBC 16.20.00.12] [Er
or 9719] [SQLState HY000] QVCI feature is disabled.; nested exception is java.sql.SQLException: [Teradata Database] [TeraJDBC 16.20.00.12] [Error 9719] [SQLState HY000] QVCI feature is disabled.
at org.springframework.jdbc.support.AbstractFallbackSQLExceptionTranslator.translate(AbstractFallbackSQLExceptionTranslator.java:89)
at org.springframework.jdbc.support.AbstractFallbackSQLExceptionTranslator.translate(AbstractFallbackSQLExceptionTranslator.java:81)
at org.springframework.jdbc.support.AbstractFallbackSQLExceptionTranslator.translate(AbstractFallbackSQLExceptionTranslator.java:81)
at org.springframework.jdbc.core.JdbcTemplate.translateException(JdbcTemplate.java:1414)
at org.springframework.jdbc.core.JdbcTemplate.execute(JdbcTemplate.java:388)
at org.springframework.jdbc.core.JdbcTemplate.query(JdbcTemplate.java:452)
at com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask.doRun(JdbcSelectTask.java:102)
at com.google.edwmigration.dumper.application.dumper.task.AbstractTask.run(AbstractTask.java:63)
at com.google.edwmigration.dumper.application.dumper.task.Task.run(Task.java:89)
at com.google.edwmigration.dumper.application.dumper.MetadataDumper.run(MetadataDumper.java:130)
at com.google.edwmigration.dumper.application.dumper.MetadataDumper.run(MetadataDumper.java:76)
at com.google.edwmigration.dumper.application.dumper.MetadataDumper.main(MetadataDumper.java:167)
Caused by: java.sql.SQLException: [Teradata Database] [TeraJDBC 16.20.00.12] [Error 9719] [SQLState HY000] QVCI feature is disabled.
at com.teradata.jdbc.jdbc_4.util.ErrorFactory.makeDatabaseSQLException(ErrorFactory.java:309)
at com.teradata.jdbc.jdbc_4.statemachine.ReceiveInitSubState.action(ReceiveInitSubState.java:103)
at com.teradata.jdbc.jdbc_4.statemachine.StatementReceiveState.subStateMachine(StatementReceiveState.java:311)
at com.teradata.jdbc.jdbc_4.statemachine.StatementReceiveState.action(StatementReceiveState.java:200)
at com.teradata.jdbc.jdbc_4.statemachine.StatementController.runBody(StatementController.java:137)
at com.teradata.jdbc.jdbc_4.statemachine.StatementController.run(StatementController.java:128)
at com.teradata.jdbc.jdbc_4.TDStatement.executeStatement(TDStatement.java:389)
at com.teradata.jdbc.jdbc_4.TDStatement.executeStatement(TDStatement.java:331)
at com.teradata.jdbc.jdbc_4.TDStatement.doNonPrepExecuteQuery(TDStatement.java:319)
at com.teradata.jdbc.jdbc_4.TDStatement.executeQuery(TDStatement.java:1121)
at org.springframework.jdbc.core.JdbcTemplate$1QueryStatementCallback.doInStatement(JdbcTemplate.java:439)
at org.springframework.jdbc.core.JdbcTemplate.execute(JdbcTemplate.java:376)
... 7 common frames omitted
 */
