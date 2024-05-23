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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.edwmigration.dumper.application.dumper.test.DumperTestUtils.assertQueryEquals;
import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorExecutionTest;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.io.FileSystemOutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.test.DummyTaskRunContext;
import com.google.edwmigration.dumper.application.dumper.test.DumperTestUtils;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataLogsDumpFormat;
import java.io.File;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author shevek */
@RunWith(JUnit4.class)
public class TeradataLogsConnectorTest extends AbstractConnectorExecutionTest {

  private final TeradataLogsConnector connector = new TeradataLogsConnector();

  @Test
  public void addTasksTo_commonConnectorTest_success() throws Exception {
    testConnectorDefaults(connector);
  }

  @Test
  public void addTasksTo_executeQuery_success() throws Exception {
    String name = getClass().getSimpleName();
    File dbFile = DumperTestUtils.newJdbcFile(name);
    File zipFile = DumperTestUtils.newZipFile(name);

    dbFile.delete();
    zipFile.delete();

    URI outputUri = URI.create("jar:" + zipFile.toURI());

    // This isn't great because all the column-validity queries fail.
    try (JdbcHandle handle = DumperTestUtils.newJdbcHandle(dbFile);
        FileSystem fileSystem =
            FileSystems.newFileSystem(outputUri, ImmutableMap.of("create", "true"))) {
      OutputHandleFactory sinkFactory = new FileSystemOutputHandleFactory(fileSystem, "/");
      handle.getJdbcTemplate().execute("attach ':memory:' as dbc");
      handle
          .getJdbcTemplate()
          .execute(
              "create table "
                  + TeradataLogsConnector.DEF_LOG_TABLE
                  + " (UserName varchar, errorcode int, StartTime int)");

      TaskRunContext runContext = new DummyTaskRunContext(sinkFactory, handle);
      List<Task<?>> tasks = new ArrayList<>();
      connector.addTasksTo(
          tasks,
          new ConnectorArguments("--connector", connector.getName(), "--query-log-days", "1"));
      for (Task<?> task : tasks) {
        task.run(runContext);
      }
    }
  }

  @Test
  public void addTasksTo_containsDumpMetadataTask() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, new ConnectorArguments("--connector", connector.getName()));

    // Assert
    assertEquals(1, tasks.stream().filter(task -> task instanceof DumpMetadataTask).count());
  }

  @Test
  public void addTasksTo_containsFormatTask() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, new ConnectorArguments("--connector", connector.getName()));

    // Assert
    assertEquals(1, tasks.stream().filter(task -> task instanceof FormatTask).count());
  }

  @Test
  public void addTasksTo_noAssessmentFlag_assessmentNotAddedToRelevantTasks() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, new ConnectorArguments("--connector", connector.getName()));

    // Assert
    assertTrue(tasks.stream().noneMatch(task -> task instanceof TeradataAssessmentLogsJdbcTask));
  }

  @Test
  public void addTasksTo_withAssessmentFlag_assessmentAddedToAllRelevantTasks() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(
        tasks, new ConnectorArguments("--connector", connector.getName(), "--assessment"));

    // Assert
    assertTrue(
        tasks.stream()
            .filter(task -> task instanceof TeradataLogsJdbcTask)
            .allMatch(task -> task instanceof TeradataAssessmentLogsJdbcTask));
  }

  @Test
  public void addTasksTo_teradataLogsJdbcTaskForOneHour() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(
        tasks,
        new ConnectorArguments(
            "--connector",
            connector.getName(),
            "--query-log-start",
            "2023-12-22 00:00:00",
            "--query-log-end",
            "2023-12-22 01:00:00"));

    // Assert
    List<String> queries =
        tasks.stream()
            .filter(task -> task instanceof TeradataLogsJdbcTask)
            .map(
                task ->
                    ((TeradataLogsJdbcTask) task)
                        .getOrCreateSql(unused -> true, ImmutableList.of("SampleColumn")))
            .collect(toImmutableList());
    assertEquals(1, queries.size());
    assertQueryEquals(
        "SELECT SampleColumn FROM dbc.DBQLogTbl L WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-12-22T00:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-12-22T01:00:00Z' AS TIMESTAMP) AND L.UserName <> 'DBC'",
        getOnlyElement(queries));
  }

  @Test
  public void addTasksTo_teradataLogsJdbcTasksForTwoHours() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(
        tasks,
        new ConnectorArguments(
            "--connector",
            connector.getName(),
            "--query-log-start",
            "2023-12-22 00:00:00",
            "--query-log-end",
            "2023-12-22 02:00:00"));

    // Assert
    List<String> queries =
        tasks.stream()
            .filter(task -> task instanceof TeradataLogsJdbcTask)
            .map(
                task ->
                    ((TeradataLogsJdbcTask) task)
                        .getOrCreateSql(unused -> true, ImmutableList.of("SampleColumn")))
            .collect(toImmutableList());
    assertEquals(
        ImmutableList.of(
            "SELECT SampleColumn FROM dbc.DBQLogTbl L WHERE L.ErrorCode=0 AND"
                + " L.StartTime >= CAST('2023-12-22T00:00:00Z' AS TIMESTAMP) AND"
                + " L.StartTime < CAST('2023-12-22T01:00:00Z' AS TIMESTAMP) AND L.UserName <> 'DBC'",
            "SELECT SampleColumn FROM dbc.DBQLogTbl L WHERE L.ErrorCode=0 AND"
                + " L.StartTime >= CAST('2023-12-22T01:00:00Z' AS TIMESTAMP) AND"
                + " L.StartTime < CAST('2023-12-22T02:00:00Z' AS TIMESTAMP) AND L.UserName <> 'DBC'"),
        queries);
  }

  @Test
  public void addTasksTo_teradataAssessmentLogsJdbcTaskWithAssessmentFlag() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(
        tasks,
        new ConnectorArguments(
            "--connector",
            connector.getName(),
            "--query-log-start",
            "2023-12-22 00:00:00",
            "--query-log-end",
            "2023-12-22 01:00:00",
            "--assessment"));

    // Assert
    List<String> queries =
        tasks.stream()
            .filter(task -> task instanceof TeradataAssessmentLogsJdbcTask)
            .map(
                task ->
                    ((TeradataAssessmentLogsJdbcTask) task)
                        .getOrCreateSql(unused -> true, ImmutableList.of("ST.QueryID")))
            .collect(toImmutableList());
    assertEquals(1, queries.size());
    assertQueryEquals(
        "SELECT ST.QueryID"
            + " FROM dbc.QryLogV L LEFT OUTER JOIN dbc.DBQLSQLTbl ST ON (L.QueryID=ST.QueryID)"
            + " WHERE L.ErrorCode=0"
            + " AND L.StartTime >= CAST('2023-12-22T00:00:00Z' AS TIMESTAMP)"
            + " AND L.StartTime < CAST('2023-12-22T01:00:00Z' AS TIMESTAMP)"
            + " ORDER BY ST.QueryID, ST.SQLRowNo",
        getOnlyElement(queries));
  }

  // exprs are usuall names, .. if not this test is junk
  private void checkNames(Enum<?>[] vals, String[] exprs) {
    assertEquals(vals.length, exprs.length);
    for (int j = 0; j < vals.length; j++) Assert.assertTrue(exprs[j].endsWith(vals[j].name()));
  }

  private void checkNames(Enum<?>[] vals, ImmutableList<String> expressions) {
    checkNames(vals, expressions.toArray(new String[0]));
  }

  @Test
  public void getDefaultFileName_forAssessment_success() {
    Instant instant = Instant.ofEpochMilli(1715346130945L);
    Clock clock = Clock.fixed(instant, UTC);

    String fileName = connector.getDefaultFileName(/* isAssessment= */ true, clock);

    assertEquals("dwh-migration-teradata-logs-logs-20240510T130210.zip", fileName);
  }

  @Test
  public void getDefaultFileName_notForAssessment_success() {
    Instant instant = Instant.ofEpochMilli(1715346130945L);
    Clock clock = Clock.fixed(instant, UTC);

    String fileName = connector.getDefaultFileName(/* isAssessment= */ false, clock);

    assertEquals("dwh-migration-teradata-logs-logs.zip", fileName);
  }

  @Test
  public void values_success() {
    checkNames(TeradataLogsDumpFormat.Header.values(), TeradataLogsJdbcTask.EXPRESSIONS);
    checkNames(
        TeradataLogsDumpFormat.HeaderLSql.values(),
        Teradata14LogsConnector.EXPRESSIONS_LSQL_TBL.toArray(
            new String[Teradata14LogsConnector.EXPRESSIONS_LSQL_TBL.size()]));
    checkNames(
        TeradataLogsDumpFormat.HeaderLog.values(),
        Teradata14LogsConnector.EXPRESSIONS_LOG_TBL.toArray(
            new String[Teradata14LogsConnector.EXPRESSIONS_LOG_TBL.size()]));
  }
}
