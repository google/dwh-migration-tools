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
package com.google.edwmigration.dumper.application.dumper.connector.airflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.junit.Test;

public class AirflowConnectorTest {

  private final AirflowConnector connector = new AirflowConnector();

  private final String validRequiredArgs =
      "--connector airflow --assessment --driver /home/dir/ --user dbadmin --password"
          + " --host localhost --port 8080 --schema airflow_db";

  // todo add interval tasks tests

  @Test
  public void validate_startDateAndLookbackDays_success() throws Exception {
    String argsStr = validRequiredArgs + " --start-date=2001-02-20 --lookback-days=25";

    // Act
    connector.validate(args(argsStr));
  }

  @Test
  public void validate_startDateAndEndDate_success() throws Exception {
    String argsStr = validRequiredArgs + " --start-date=2001-02-20 --end-date=2001-02-25";

    // Act
    connector.validate(args(argsStr));
  }

  @Test
  public void validate_startDateAfterEndDate_throws() {
    String argsStr = validRequiredArgs + " --start-date=2001-02-20 --end-date=2001-02-20";

    Exception exception =
        assertThrows(IllegalStateException.class, () -> connector.validate(args(argsStr)));
    assertEquals(
        "Start date [2001-02-20T00:00Z] must be before end date [2001-02-20T00:00Z].",
        exception.getMessage());
  }

  @Test
  public void validate_allDateOptions_throws() {
    String argsStr =
        validRequiredArgs + " --start-date=2001-02-20 --end-date=2001-02-25 --lookback-days=10";

    Exception exception =
        assertThrows(IllegalStateException.class, () -> connector.validate(args(argsStr)));
    assertEquals(
        "Incompatible options, either specify a number of days to export or a end date.",
        exception.getMessage());
  }

  @Test
  public void validate_endDateAlone_throws() {
    String argsStr = validRequiredArgs + " --end-date=2001-02-20";

    Exception exception =
        assertThrows(IllegalStateException.class, () -> connector.validate(args(argsStr)));
    assertEquals(
        "End date can be specified only with start date, but start date was null.",
        exception.getMessage());
  }

  @Test
  public void validate_startDateAlone_throws() {
    String argsStr = validRequiredArgs + " --start-date=2001-02-20";

    Exception exception =
        assertThrows(IllegalStateException.class, () -> connector.validate(args(argsStr)));
    assertEquals(
        "Incompatible options, number of days or end date must be specified with start date.",
        exception.getMessage());
  }

  @Test
  public void validate_lookbackDay_nonPositiveThrows() throws Exception {
    String argsStr = validRequiredArgs + " --lookback-days=0";

    // Act
    Exception exception =
        assertThrows(IllegalStateException.class, () -> connector.validate(args(argsStr)));
    assertEquals("Number of days to export must be 1 or greater.", exception.getMessage());
  }

  @Test
  public void validate_lookbackDay_success() throws Exception {
    String argsStr = validRequiredArgs + " --lookback-days=5";
    // Act
    connector.validate(args(argsStr));
  }

  @Test
  public void validate_databaseParam_isNotSupported() throws Exception {
    String argsStr =
        "--connector airflow --assessment --driver /home/dir/ --user dbadmin --password"
            + " --url localhost"
            + " --database myDB";

    // Act
    Exception exception =
        assertThrows(IllegalStateException.class, () -> connector.validate(args(argsStr)));
    assertEquals("--database is not supported, use --schema or --url", exception.getMessage());
  }

  @Test
  public void validate_hostParam_success() throws Exception {
    String argsStr =
        "--connector airflow --assessment --driver /home/dir/ --user dbadmin --password"
            + " --host localhost --port 8080 --schema airflow_db";

    // Act
    connector.validate(args(argsStr));
  }

  @Test
  public void validate_hostParam_brokenState() throws Exception {
    String argsStr =
        "--connector airflow --assessment --driver /home/dir/ --user dbadmin --password";

    // Act
    {
      String argsWithHost = argsStr + " --host localhost ";
      Exception exception =
          assertThrows(IllegalStateException.class, () -> connector.validate(args(argsWithHost)));
      assertEquals("--port is required with --host", exception.getMessage());
    }
    {
      String argsWithPort = argsStr + " --host localhost --port 8080";
      Exception exception =
          assertThrows(IllegalStateException.class, () -> connector.validate(args(argsWithPort)));
      assertEquals("--schema is required with --host", exception.getMessage());
    }
    {
      String argsWithPort = argsStr + " --host localhost --schema 8080";
      Exception exception =
          assertThrows(IllegalStateException.class, () -> connector.validate(args(argsWithPort)));
      assertEquals("--port is required with --host", exception.getMessage());
    }
  }

  @Test
  public void validate_jdbcParam_success() throws Exception {
    String argsStr =
        "--connector airflow --driver /home/dir/ --user dbadmin --password --assessment";
    argsStr += " --url somestring";

    // Act
    connector.validate(args(argsStr));
  }

  @Test
  public void validate_jdbcParam_brokenState() throws Exception {
    String argsStr =
        "--connector airflow --assessment --driver /home/dir/ --user dbadmin --password";
    argsStr += " --url somestring";

    {
      String argsWithPort = argsStr + " --port 8080";
      Exception exception =
          assertThrows(IllegalStateException.class, () -> connector.validate(args(argsWithPort)));
      assertEquals("--port param should not be used with --url", exception.getMessage());
    }
    {
      String argsWithSchema = argsStr + " --schema 8080";
      Exception exception =
          assertThrows(IllegalStateException.class, () -> connector.validate(args(argsWithSchema)));
      assertEquals("--schema param should not be used with --url", exception.getMessage());
    }
    {
      String argsWithSchema = argsStr + " --host localhost";
      Exception exception =
          assertThrows(IllegalStateException.class, () -> connector.validate(args(argsWithSchema)));
      assertEquals(
          "--url either --host must be provided (both parameters at once are not acceptable)",
          exception.getMessage());
    }
  }

  @Test
  public void validate_requiredArguments() throws Exception {
    Exception exception;

    {
      String argsStr = "--connector airflow ";
      exception =
          assertThrows(IllegalStateException.class, () -> connector.validate(args(argsStr)));
      assertEquals("--assessment flag is required", exception.getMessage());
    }
    {
      String argsStr = "--connector airflow  --assessment";
      exception =
          assertThrows(IllegalStateException.class, () -> connector.validate(args(argsStr)));
      assertEquals("Path to jdbc driver is required in --driver param", exception.getMessage());
    }
    {
      String argsStr = "--connector airflow  --assessment --driver /home/dir/";
      exception =
          assertThrows(IllegalStateException.class, () -> connector.validate(args(argsStr)));
      assertEquals("--user param is required", exception.getMessage());
    }
    {
      String argsStr = "--connector airflow --driver /home/dir/ --user dbusername --assessment";
      exception =
          assertThrows(IllegalStateException.class, () -> connector.validate(args(argsStr)));
      assertEquals("--password param is required", exception.getMessage());
    }
    {
      String argsStr =
          "--connector airflow " + "--driver /home/dir/ --user dbusername --assessment --password";
      exception =
          assertThrows(IllegalStateException.class, () -> connector.validate(args(argsStr)));
      assertEquals(
          "--url either --host must be provided (both parameters at once are not acceptable)",
          exception.getMessage());
    }
  }

  @Test
  public void addTasksTo_containsJdbcSelect_required() throws Exception {
    Map<String, String> expectedFilesToTables =
        ImmutableMap.of(
            "dag.csv", "dag",
            "task_instance.csv", "task_instance",
            "dag_run.csv", "dag_run");

    testJdbcSelectTasks(expectedFilesToTables, TaskCategory.REQUIRED);
  }

  @Test
  public void addTasksTo_containsJdbcSelect_optional() throws Exception {
    Map<String, String> expectedFilesToTables =
        ImmutableMap.of("serialized_dag.csv", "serialized_dag", "dag_code.csv", "dag_code");

    testJdbcSelectTasks(expectedFilesToTables, TaskCategory.OPTIONAL);
  }

  private void testJdbcSelectTasks(
      Map<String, String> expectedFilesToTables, TaskCategory taskCategory) throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, args(validRequiredArgs));

    // Assert
    Map<String, String> existingFilesToTables =
        tasks.stream()
            .filter(t -> t instanceof JdbcSelectTask)
            .map(JdbcSelectTask.class::cast)
            .filter(t -> t.getCategory().equals(taskCategory))
            .collect(Collectors.toMap(JdbcSelectTask::getTargetPath, JdbcSelectTask::getSql));

    assertEquals(expectedFilesToTables.size(), existingFilesToTables.size());
    for (Entry<String, String> entry : expectedFilesToTables.entrySet()) {
      String fileName = entry.getKey();
      String tableName = entry.getValue();

      String sql = existingFilesToTables.get(fileName);
      assertNotNull("Query for file " + fileName + " doesn't exist", sql);

      assertTrue(
          "file: " + fileName + " should be selected from table: " + tableName,
          sql.toLowerCase().contains("from " + tableName.toLowerCase()));
    }
  }

  @Test
  public void addTasksTo_containsRequiredTasks() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, args(validRequiredArgs));

    // Assert
    long dumpMetadataCount = tasks.stream().filter(t -> t instanceof DumpMetadataTask).count();
    long formatCount = tasks.stream().filter(t -> t instanceof FormatTask).count();

    assertEquals("One DumpMetadataTask is expected", dumpMetadataCount, 1);
    assertEquals("One FormatTask is expected", formatCount, 1);
  }

  private static ConnectorArguments args(String args) throws Exception {
    return new ConnectorArguments(args.split(" "));
  }
}
