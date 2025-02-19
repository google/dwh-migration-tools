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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.junit.Assert.assertEquals;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorExecutionTest;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author shevek */
@RunWith(JUnit4.class)
public class RedshiftServerlessLogsConnectorTest extends AbstractConnectorExecutionTest {

  private final RedshiftServerlessLogsConnector connector = new RedshiftServerlessLogsConnector();

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
  public void addTasksTo_containsJdbcServerlessUsageTask() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, new ConnectorArguments("--connector", connector.getName()));

    // Assert
    List<String> queries =
        tasks.stream()
            .filter(task -> task instanceof JdbcSelectTask)
            .map(task -> ((JdbcSelectTask) task).getSql())
            .collect(toImmutableList());
    assertEquals(1, queries.size());
    assertEquals("SELECT * FROM SYS_SERVERLESS_USAGE", getOnlyElement(queries));
  }
}
