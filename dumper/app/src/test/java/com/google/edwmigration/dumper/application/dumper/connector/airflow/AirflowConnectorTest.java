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
package com.google.edwmigration.dumper.application.dumper.connector.airflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
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

  @Test
  public void addTasksTo_containsJdbcSelect_required() throws Exception {
    Map<String, String> expectedFilesToTables =
        ImmutableMap.of(
            "dag.csv", "dag",
            "dag_run.csv", "dag_run",
            "job.csv", "job",
            "serialized_dag.csv", "serialized_dag",
            "task_instance.csv", "task_instance");

    testJdbcSelectTasks(expectedFilesToTables, TaskCategory.REQUIRED);
  }

  @Test
  public void addTasksTo_containsJdbcSelect_optional() throws Exception {
    Map<String, String> expectedFilesToTables =
        ImmutableMap.of("task_instance_history.csv", "task_instance_history");

    testJdbcSelectTasks(expectedFilesToTables, TaskCategory.OPTIONAL);
  }

  private void testJdbcSelectTasks(
      Map<String, String> expectedFilesToTables, TaskCategory taskCategory) throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, null);

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
    connector.addTasksTo(tasks, null);

    // Assert
    long dumpMetadataCount = tasks.stream().filter(t -> t instanceof DumpMetadataTask).count();
    long formatCount = tasks.stream().filter(t -> t instanceof FormatTask).count();

    assertEquals("One DumpMetadataTask is expected", dumpMetadataCount, 1);
    assertEquals("One FormatTask is expected", formatCount, 1);
  }
}
