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
package com.google.edwmigration.dumper.application.dumper.connector.hive;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorTest;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class HiveMetadataConnectorTest extends AbstractConnectorTest {

  private final HiveMetadataConnector connector = new HiveMetadataConnector();

  @Test
  public void testConnector() throws Exception {
    testConnectorDefaults(connector);
  }

  @DataPoints("commonTaskNames")
  public static final ImmutableList<String> EXPECTED_TASK_NAMES =
      ImmutableList.of(
          "compilerworks-metadata.yaml",
          "compilerworks-format.txt",
          "schemata.csv",
          "functions.csv");

  @Theory
  public void addTasksTo_taskExists_success(
      @FromDataPoints("commonTaskNames") String taskName, boolean migrationMetadataEnabled)
      throws IOException {
    ConnectorArguments args =
        new ConnectorArguments(
            "--connector", "hiveql", "-Dhiveql.migration.metadata=" + migrationMetadataEnabled);
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, args);

    // Assert
    ImmutableList<String> taskNames = tasks.stream().map(Task::getName).collect(toImmutableList());
    assertTrue(
        "Task names must contain '" + taskName + "'. Actual tasks: " + taskNames,
        taskNames.contains(taskName));
  }

  @Test
  public void addTasksTo_originalTableJsonl_success() throws IOException {
    ConnectorArguments args = new ConnectorArguments("--connector", "hiveql");
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, args);

    // Assert
    ImmutableList<String> taskNames = tasks.stream().map(Task::getName).collect(toImmutableList());
    assertTrue(
        "Task names must contain 'tables.jsonl'. Actual tasks: " + taskNames,
        taskNames.contains("tables.jsonl"));
  }

  @DataPoints("migrationMetadataTaskNames")
  public static final ImmutableList<String> EXPECTED_TASK_NAMES_WITH_MIGRATION_METADATA_ENABLED =
      ImmutableList.of(
          "catalogs.jsonl",
          "databases.jsonl",
          "master-keys.jsonl",
          "delegation-tokens.jsonl",
          "functions.jsonl",
          "resource-plans.jsonl",
          "tables-raw.jsonl");

  @Theory
  public void addTasksTo_migrationMetadataTaskExists_success(
      @FromDataPoints("migrationMetadataTaskNames") String taskName) throws IOException {
    ConnectorArguments args =
        new ConnectorArguments("--connector", "hiveql", "-Dhiveql.migration.metadata=true");
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, args);

    // Assert
    ImmutableList<String> taskNames = tasks.stream().map(Task::getName).collect(toImmutableList());
    assertTrue(
        "Task names must contain '" + taskName + "'. Actual tasks: " + taskNames,
        taskNames.contains(taskName));
  }
}
