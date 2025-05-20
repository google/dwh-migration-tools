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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

public class ClouderaManagerConnectorTest {
  private final Connector connector = new ClouderaManagerConnector();

  @Test
  public void addTasksTo_checkFilesCategory() throws Exception {
    Map<String, TaskCategory> expectedFilesToCategory =
        ImmutableMap.<String, TaskCategory>builder()
            .putAll(
                ImmutableMap.of(
                    "compilerworks-metadata.yaml", TaskCategory.REQUIRED,
                    "compilerworks-format.txt", TaskCategory.REQUIRED,
                    "clusters.json", TaskCategory.REQUIRED,
                    "cmf-hosts.jsonl", TaskCategory.OPTIONAL,
                    "api-hosts.jsonl", TaskCategory.REQUIRED,
                    "services.jsonl", TaskCategory.REQUIRED,
                    "host-components.jsonl", TaskCategory.OPTIONAL))
            .putAll(
                ImmutableMap.of(
                    "cluster-cpu-1d.jsonl", TaskCategory.REQUIRED,
                    "cluster-cpu-7d.jsonl", TaskCategory.OPTIONAL,
                    "cluster-cpu-30d.jsonl", TaskCategory.OPTIONAL,
                    "cluster-cpu-90d.jsonl", TaskCategory.OPTIONAL))
            .putAll(
                ImmutableMap.of(
                    "host-ram-1d.jsonl", TaskCategory.REQUIRED,
                    "host-ram-7d.jsonl", TaskCategory.OPTIONAL,
                    "host-ram-30d.jsonl", TaskCategory.OPTIONAL,
                    "host-ram-90d.jsonl", TaskCategory.OPTIONAL))
            .putAll(
                ImmutableMap.of(
                    "yarn-applications-90d.jsonl", TaskCategory.OPTIONAL,
                    "yarn-application-types-90d.jsonl", TaskCategory.OPTIONAL))
            .build();
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, null);

    // Assert
    Map<String, TaskCategory> filesToCategory =
        tasks.stream().collect(Collectors.toMap(Task::getTargetPath, Task::getCategory));
    assertEquals(expectedFilesToCategory, filesToCategory);
  }

  @Test
  public void addTasksTo_containsDefaultTasks() throws Exception {
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
