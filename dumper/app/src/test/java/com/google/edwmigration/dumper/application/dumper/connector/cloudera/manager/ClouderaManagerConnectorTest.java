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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Test;
import org.mockito.MockedStatic;

public class ClouderaManagerConnectorTest {
  private static final String clouderaRequiredArgs =
      "--connector simple --url some/url --user user --password secret";
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
                    "cluster-cpu.jsonl", TaskCategory.REQUIRED,
                    "host-ram.jsonl", TaskCategory.REQUIRED,
                    "service-resource-allocation.jsonl", TaskCategory.OPTIONAL,
                    "yarn-applications.jsonl", TaskCategory.OPTIONAL,
                    "yarn-application-types.jsonl", TaskCategory.OPTIONAL))
            .build();
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(
        tasks, new ConnectorArguments("--connector", "simple", "--password", "secret"));

    // Assert
    Map<String, TaskCategory> filesToCategory =
        tasks.stream().collect(Collectors.toMap(Task::getTargetPath, Task::getCategory));
    assertEquals(expectedFilesToCategory, filesToCategory);
  }

  @Test
  public void addTasksTo_checkFileWasGeneratedByProperTask() throws Exception {
    Map<String, Class<?>> expectedFileToTaskType =
        ImmutableMap.<String, Class<?>>builder()
            .putAll(
                ImmutableMap.of(
                    "compilerworks-metadata.yaml", DumpMetadataTask.class,
                    "compilerworks-format.txt", FormatTask.class,
                    "clusters.json", ClouderaClustersTask.class,
                    "cmf-hosts.jsonl", ClouderaCMFHostsTask.class,
                    "api-hosts.jsonl", ClouderaAPIHostsTask.class,
                    "services.jsonl", ClouderaServicesTask.class))
            .putAll(
                ImmutableMap.of(
                    "host-components.jsonl", ClouderaHostComponentsTask.class,
                    "cluster-cpu.jsonl", ClouderaClusterCPUChartTask.class,
                    "host-ram.jsonl", ClouderaHostRAMChartTask.class,
                    "service-resource-allocation.jsonl",
                        ClouderaServiceResourceAllocationChartTask.class,
                    "yarn-applications.jsonl", ClouderaYarnApplicationsTask.class,
                    "yarn-application-types.jsonl", ClouderaYarnApplicationTypeTask.class))
            .build();
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(
        tasks, new ConnectorArguments("--connector", "simple", "--password", "secret"));

    // Assert
    tasks.forEach(
        task -> {
          String targetPath = task.getTargetPath();
          Class<?> expectedTaskType = expectedFileToTaskType.get(targetPath);
          assertNotNull("Unexpected file has been generated.", expectedTaskType);
          assertEquals(
              String.format(
                  "File %s has to be generated by %s task but was generated by %s.",
                  targetPath, expectedTaskType.getName(), task.getClass().getName()),
              task.getClass(),
              expectedTaskType);
        });
  }

  @Test
  public void addTasksTo_checkFilesCategoryWithCustomDateRange() throws Exception {
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
                    "cluster-cpu.jsonl", TaskCategory.REQUIRED,
                    "host-ram.jsonl", TaskCategory.REQUIRED,
                    "service-resource-allocation.jsonl", TaskCategory.OPTIONAL,
                    "yarn-applications.jsonl", TaskCategory.OPTIONAL,
                    "yarn-application-types.jsonl", TaskCategory.OPTIONAL))
            .build();
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(
        tasks,
        new ConnectorArguments(
            "--connector",
            "simple",
            "--password",
            "secret",
            "--start-date",
            "2025-04-01 00:00:00.000",
            "--end-date",
            "2025-04-20 00:00:00.000"));

    // Assert
    Map<String, TaskCategory> filesToCategory =
        tasks.stream().collect(Collectors.toMap(Task::getTargetPath, Task::getCategory));
    assertEquals(expectedFilesToCategory, filesToCategory);
  }

  @Test
  public void addTasksTo_checkFileWasGeneratedByProperTaskWithCustomDateRange() throws Exception {
    Map<String, Class<?>> expectedFileToTaskType =
        ImmutableMap.<String, Class<?>>builder()
            .putAll(
                ImmutableMap.of(
                    "compilerworks-metadata.yaml", DumpMetadataTask.class,
                    "compilerworks-format.txt", FormatTask.class,
                    "clusters.json", ClouderaClustersTask.class,
                    "cmf-hosts.jsonl", ClouderaCMFHostsTask.class,
                    "api-hosts.jsonl", ClouderaAPIHostsTask.class,
                    "services.jsonl", ClouderaServicesTask.class,
                    "host-components.jsonl", ClouderaHostComponentsTask.class))
            .putAll(
                ImmutableMap.of(
                    "cluster-cpu.jsonl", ClouderaClusterCPUChartTask.class,
                    "host-ram.jsonl", ClouderaHostRAMChartTask.class,
                    "service-resource-allocation.jsonl",
                        ClouderaServiceResourceAllocationChartTask.class,
                    "yarn-applications.jsonl", ClouderaYarnApplicationsTask.class,
                    "yarn-application-types.jsonl", ClouderaYarnApplicationTypeTask.class))
            .build();
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(
        tasks,
        new ConnectorArguments(
            "--connector",
            "simple",
            "--password",
            "secret",
            "--start-date",
            "2025-04-01 00:00:00.000",
            "--end-date",
            "2025-04-20 00:00:00.000"));

    // Assert
    tasks.forEach(
        task -> {
          String targetPath = task.getTargetPath();
          Class<?> expectedTaskType = expectedFileToTaskType.get(targetPath);
          assertNotNull("Unexpected file has been generated: " + targetPath, expectedTaskType);
          assertEquals(
              String.format(
                  "File %s has to be generated by %s task but was generated by %s.",
                  targetPath, expectedTaskType.getName(), task.getClass().getName()),
              task.getClass(),
              expectedTaskType);
        });
  }

  @Test
  public void addTasksTo_useOnlyEndDate_throwsException() {
    String args = clouderaRequiredArgs + " --end-date 2025-04-01 00:00:00.000";

    // Act
    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> connector.validate(args(args)));

    // Assert
    assertEquals(
        "End date can be specified only with start date, but start date was null.",
        exception.getMessage());
  }

  @Test
  public void addTasksTo_useOnlyStartDate_throwsException() {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                connector.addTasksTo(
                    tasks,
                    args(
                        "--connector simple --password secret --start-date 2025-04-01 00:00:00.000")));

    // Assert
    assertEquals("End date must be not null.", exception.getMessage());
  }

  @Test
  public void addTasksTo_containsDefaultTasks() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, args("--connector simple --password secret"));

    // Assert
    long dumpMetadataCount = tasks.stream().filter(t -> t instanceof DumpMetadataTask).count();
    long formatCount = tasks.stream().filter(t -> t instanceof FormatTask).count();

    assertEquals("One DumpMetadataTask is expected", dumpMetadataCount, 1);
    assertEquals("One FormatTask is expected", formatCount, 1);
  }

  @Test
  public void open_success() throws Exception {
    try (MockedStatic<ClouderaManagerLoginHelper> loginHelper =
            mockStatic(ClouderaManagerLoginHelper.class);
        MockedStatic<ClouderaConnectorVerifier> connectorVerifier =
            mockStatic(ClouderaConnectorVerifier.class)) {
      ConnectorArguments arguments =
          new ConnectorArguments(
              "--connector",
              "cloudera-manager",
              "--url",
              "http://localhost",
              "--user",
              "user",
              "--password",
              "password");
      ClouderaManagerConnector connector = new ClouderaManagerConnector();

      // Act
      ClouderaManagerHandle handle = connector.open(arguments);

      // Assert
      assertNotNull(handle);
      loginHelper.verify(
          () ->
              ClouderaManagerLoginHelper.login(
                  any(URI.class), any(CloseableHttpClient.class), eq("user"), eq("password")));
      connectorVerifier.verify(() -> ClouderaConnectorVerifier.verify(any(), any()));
    }
  }

  @Test
  public void open_verifierFails_throwsException() throws Exception {
    try (MockedStatic<ClouderaManagerLoginHelper> loginHelper =
            mockStatic(ClouderaManagerLoginHelper.class);
        MockedStatic<ClouderaConnectorVerifier> connectorVerifier =
            mockStatic(ClouderaConnectorVerifier.class)) {
      ConnectorArguments arguments =
          new ConnectorArguments(
              "--connector",
              "cloudera-manager",
              "--url",
              "http://localhost",
              "--user",
              "user",
              "--password",
              "password");
      ClouderaManagerConnector connector = new ClouderaManagerConnector();

      RuntimeException expectedException = new RuntimeException("Verification failed");
      connectorVerifier
          .when(() -> ClouderaConnectorVerifier.verify(any(), any()))
          .thenThrow(expectedException);

      // Act & Assert
      RuntimeException actualException =
          assertThrows(RuntimeException.class, () -> connector.open(arguments));
      assertEquals(expectedException, actualException);

      loginHelper.verify(
          () ->
              ClouderaManagerLoginHelper.login(
                  any(URI.class), any(CloseableHttpClient.class), eq("user"), eq("password")));
      connectorVerifier.verify(() -> ClouderaConnectorVerifier.verify(any(), any()));
    }
  }

  private static ConnectorArguments args(String s) throws Exception {
    return new ConnectorArguments(s.split(" "));
  }
}
