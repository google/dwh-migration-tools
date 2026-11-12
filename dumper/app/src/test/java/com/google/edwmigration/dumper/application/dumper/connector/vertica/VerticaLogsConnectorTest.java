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
package com.google.edwmigration.dumper.application.dumper.connector.vertica;

import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_QUERY_LOG_EARLIEST_TIMESTAMP;
import static org.junit.Assert.assertTrue;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class VerticaLogsConnectorTest {

  private final VerticaLogsConnector connector = new VerticaLogsConnector();

  @Test
  public void addTasksTo_earliestTimestamp_comparedAsTimestampLiteral() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            connector.getName(),
            "--" + OPT_QUERY_LOG_EARLIEST_TIMESTAMP,
            "2024-03-21T10:15");

    connector.addTasksTo(tasks, arguments);

    String descriptions = tasks.toString();
    assertTrue(
        descriptions,
        descriptions.contains(
            "SELECT * FROM dc_requests_issued WHERE time >= CAST('2024-03-21 10:15:00' AS TIMESTAMP)"));
    assertTrue(
        descriptions,
        descriptions.contains(
            "SELECT * FROM dc_requests_completed WHERE time >= CAST('2024-03-21 10:15:00' AS TIMESTAMP)"));
  }
}
