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
package com.google.edwmigration.dumper.application.dumper.connector.databricks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksConnectorTest {

  @Test
  public void addTasksTo_commonConnectorTest_success() throws Exception {
    DatabricksConnector connector = new DatabricksConnector();
    List<com.google.edwmigration.dumper.application.dumper.task.Task<?>> tasks = new ArrayList<>();

    connector.addTasksTo(tasks, new ConnectorArguments("--connector", connector.getName()));

    assertTrue(tasks.stream().map(Task::getName).anyMatch("catalogs.jsonl"::equals));
    assertTrue(tasks.stream().map(Task::getName).anyMatch("databases.jsonl"::equals));
    assertTrue(tasks.stream().map(Task::getName).anyMatch("tables-raw.jsonl"::equals));
    assertTrue(
        "Expected at least one DumpMetadataTask",
        tasks.stream().anyMatch(task -> task instanceof DumpMetadataTask));
    assertTrue(
        "Expected at least one FormatTask", tasks.stream().anyMatch(task -> task instanceof FormatTask));

    // Name should be the known connector name
    assertEquals("databricks", connector.getName());
  }
}
