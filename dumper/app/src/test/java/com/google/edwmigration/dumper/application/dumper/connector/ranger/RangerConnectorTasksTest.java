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
package com.google.edwmigration.dumper.application.dumper.connector.ranger;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

@RunWith(JUnit4.class)
public class RangerConnectorTasksTest {
  @Mock protected CloseableHttpClient httpClient = mock(CloseableHttpClient.class);

  @Test
  public void isDumpMetadataTaskinConnectorTasks() throws Exception {
    RangerConnector connector = new RangerConnector();

    List<Task<?>> tasks = new ArrayList<>();
    ConnectorArguments args = new ConnectorArguments("--connector", "ranger");

    connector.addTasksTo(tasks, args);
    assertTrue(
        tasks.stream()
            .filter(task -> task.toString().contains("compilerworks-metadata.yaml"))
            .findAny()
            .isPresent());
  }
}
