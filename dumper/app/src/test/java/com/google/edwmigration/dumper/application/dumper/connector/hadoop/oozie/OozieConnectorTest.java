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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop.oozie;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.oozie.client.XOozieClient;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class OozieConnectorTest {
  private final OozieConnector connector = new OozieConnector();

  @Test
  public void addTasksTo_jobs_required() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, null);

    // Assert
    List<Task<?>> jobs =
        tasks.stream()
            .filter(t -> t.getTargetPath().equals("oozie_jobs.csv"))
            .collect(Collectors.toList());

    assertEquals(1, jobs.size());
    assertEquals(TaskCategory.REQUIRED, jobs.get(0).getCategory());
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

  @Test
  public void open_delegateToFactory_Success() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      XOozieClient oozieClient = mock(XOozieClient.class);
      factory.when(OozieClientFactory::createXOozieClient).thenReturn(oozieClient);

      // Act
      Handle handle = connector.open(null);

      assertEquals(OozieHandle.class, handle.getClass());
      assertEquals(oozieClient, ((OozieHandle) handle).getOozieClient());
    }
  }
}
