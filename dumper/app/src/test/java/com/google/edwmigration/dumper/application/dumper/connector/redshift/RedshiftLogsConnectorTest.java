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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.ParallelTaskGroup;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftLogsDumpFormat.DdlHistory;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftLogsDumpFormat.QueryHistory;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftMetadataDumpFormat.PgUser;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RedshiftLogsConnectorTest {

  private final RedshiftLogsConnector connector = new RedshiftLogsConnector();

  @Test
  public void addTasksTo_schedulesQueryLogMetadataAndFormatTasks() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    connector.addTasksTo(
        tasks, new ConnectorArguments("--connector", connector.getName(), "--query-log-days", "1"));

    assertEquals(3, tasks.size());
    assertTrue(tasks.get(0) instanceof ParallelTaskGroup);
    List<String> targetPaths = new ArrayList<>();
    for (Task<?> task : ((ParallelTaskGroup) tasks.get(0)).getTasks()) {
      targetPaths.add(task.getTargetPath());
    }
    assertTrue(targetPaths.toString(), targetPaths.contains(PgUser.ZIP_ENTRY_NAME));
    assertTrue(targetPaths.toString(), targetPaths.stream().anyMatch(DdlHistory::isZipEntryName));
    assertTrue(targetPaths.toString(), targetPaths.stream().anyMatch(QueryHistory::isZipEntryName));
    assertTrue(tasks.get(1) instanceof DumpMetadataTask);
    assertTrue(tasks.get(2) instanceof FormatTask);
  }
}
