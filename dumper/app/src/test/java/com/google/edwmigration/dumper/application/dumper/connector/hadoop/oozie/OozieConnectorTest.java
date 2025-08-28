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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.ZonedParser;
import com.google.edwmigration.dumper.application.dumper.ZonedParser.DayOffset;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.oozie.client.XOozieClient;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class OozieConnectorTest {

  private static final String validRequiredArgs = "--connector oozie --assessment";

  private final ZonedDateTime nowInTest = ZonedDateTime.now();
  private final OozieConnector connector = new OozieConnector(() -> nowInTest);

  @Test
  public void validate_startDateAndEndDate_success() throws Exception {
    String argsStr = validRequiredArgs + " --start-date=2001-02-20 --end-date=2001-02-25";

    // Act
    connector.validate(toArgs(argsStr));
  }

  @Test
  public void validate_startDateAfterEndDate_throws() {
    String argsStr = validRequiredArgs + " --start-date=2001-02-20 --end-date=2001-02-20";

    Exception exception =
        assertThrows(RuntimeException.class, () -> connector.validate(toArgs(argsStr)));
    assertEquals(
        "Start date [2001-02-20T00:00Z] must be before end date [2001-02-20T00:00Z].",
        exception.getMessage());
  }

  @Test
  public void validate_endDateAlone_throws() {
    String argsStr = validRequiredArgs + " --end-date=2001-02-20";

    Exception exception =
        assertThrows(RuntimeException.class, () -> connector.validate(toArgs(argsStr)));
    assertEquals(
        "End date can be specified only with start date, but start date was null.",
        exception.getMessage());
  }

  @Test
  public void validate_startDateAlone_throws() {
    String argsStr = validRequiredArgs + " --start-date=2001-02-20";

    Exception exception =
        assertThrows(RuntimeException.class, () -> connector.validate(toArgs(argsStr)));
    assertEquals(
        "End date must be specified with start date, but was null.", exception.getMessage());
  }

  @Test
  public void validate_requiredArgs_success() throws Exception {
    // Act
    connector.validate(toArgs(validRequiredArgs));
  }

  @Test
  public void validate_AssessmentIsRequired_throw() throws Exception {

    Exception exception =
        assertThrows(RuntimeException.class, () -> connector.validate(toArgs("--connector oozie")));

    assertEquals("--assessment flag is required", exception.getMessage());
  }

  @Test
  public void addTasksTo_checkFilesCategory() throws Exception {
    Map<String, TaskCategory> expectedFilesToCategory =
        ImmutableMap.of(
            "compilerworks-metadata.yaml", TaskCategory.REQUIRED,
            "compilerworks-format.txt", TaskCategory.REQUIRED,
            "oozie_info.csv", TaskCategory.REQUIRED,
            "oozie_coord_jobs.csv", TaskCategory.OPTIONAL,
            "oozie_bundle_jobs.csv", TaskCategory.OPTIONAL,
            "oozie_servers.csv", TaskCategory.REQUIRED,
            "oozie_workflow_jobs.csv", TaskCategory.REQUIRED);
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, toArgs("--connector oozie"));

    // Assert
    Map<String, TaskCategory> filesToCategory =
        tasks.stream().collect(Collectors.toMap(Task::getTargetPath, Task::getCategory));
    assertEquals(expectedFilesToCategory, filesToCategory);
  }

  @Test
  public void addTasksTo_containsRequiredTasks() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, toArgs("--connector oozie"));

    // Assert
    long dumpMetadataCount = tasks.stream().filter(t -> t instanceof DumpMetadataTask).count();
    long formatCount = tasks.stream().filter(t -> t instanceof FormatTask).count();

    assertEquals("One DumpMetadataTask is expected", dumpMetadataCount, 1);
    assertEquals("One FormatTask is expected", formatCount, 1);
  }

  @Test
  public void addTasksTo_dateRangeSpecified() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();
    String argsStr = validRequiredArgs + " --start-date=2001-02-20 --end-date=2001-02-21";

    // Act
    connector.addTasksTo(tasks, toArgs(argsStr));

    // Assert
    OozieWorkflowJobsTask workflowJobsTask =
        (OozieWorkflowJobsTask)
            tasks.stream().filter(t -> t instanceof OozieWorkflowJobsTask).findAny().get();
    OozieCoordinatorJobsTask coordinatorJobsTask =
        (OozieCoordinatorJobsTask)
            tasks.stream().filter(t -> t instanceof OozieCoordinatorJobsTask).findAny().get();
    OozieBundleJobsTask bundleJobsTask =
        (OozieBundleJobsTask)
            tasks.stream().filter(t -> t instanceof OozieBundleJobsTask).findAny().get();

    ZonedDateTime startDate =
        ZonedParser.withDefaultPattern(DayOffset.START_OF_DAY).convert("2001-02-20");
    ZonedDateTime endDate =
        ZonedParser.withDefaultPattern(DayOffset.START_OF_DAY).convert("2001-02-21");
    assertEquals(startDate, workflowJobsTask.getStartDate());
    assertEquals(startDate, coordinatorJobsTask.getStartDate());
    assertEquals(startDate, bundleJobsTask.getStartDate());

    assertEquals(endDate, workflowJobsTask.getEndDate());
    assertEquals(endDate, coordinatorJobsTask.getEndDate());
    assertEquals(endDate, bundleJobsTask.getEndDate());
  }

  @Test
  public void addTasksTo_dateRangeDefault() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, toArgs(validRequiredArgs));

    // Assert
    OozieWorkflowJobsTask workflowJobsTask =
        (OozieWorkflowJobsTask)
            tasks.stream().filter(t -> t instanceof OozieWorkflowJobsTask).findAny().get();
    OozieCoordinatorJobsTask coordinatorJobsTask =
        (OozieCoordinatorJobsTask)
            tasks.stream().filter(t -> t instanceof OozieCoordinatorJobsTask).findAny().get();
    OozieBundleJobsTask bundleJobsTask =
        (OozieBundleJobsTask)
            tasks.stream().filter(t -> t instanceof OozieBundleJobsTask).findAny().get();

    ZonedDateTime endDate = nowInTest;
    ZonedDateTime startDate = endDate.minusDays(93);
    assertEquals(startDate, workflowJobsTask.getStartDate());
    assertEquals(startDate, coordinatorJobsTask.getStartDate());
    assertEquals(startDate, bundleJobsTask.getStartDate());

    assertEquals(endDate, workflowJobsTask.getEndDate());
    assertEquals(endDate, coordinatorJobsTask.getEndDate());
    assertEquals(endDate, bundleJobsTask.getEndDate());
  }

  @Test
  public void open_delegateToFactoryNoArg_success() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      XOozieClient oozieClient = mock(XOozieClient.class);
      factory
          .when(() -> OozieClientFactory.createXOozieClient(null, null, null))
          .thenReturn(oozieClient);

      // Act
      Handle handle = connector.open(toArgs("--connector oozie"));

      assertEquals(OozieHandle.class, handle.getClass());
      assertEquals(oozieClient, ((OozieHandle) handle).getOozieClient());
    }
  }

  @Test
  public void open_delegateToFactoryUrlArg_success() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      XOozieClient oozieClient = mock(XOozieClient.class);
      factory
          .when(
              () ->
                  OozieClientFactory.createXOozieClient(
                      eq("https://some/path"), eq(null), eq(null)))
          .thenReturn(oozieClient);

      // Act
      String args = "--connector oozie --url https://some/path";
      Handle handle = connector.open(toArgs(args));

      assertEquals(OozieHandle.class, handle.getClass());
      assertEquals(oozieClient, ((OozieHandle) handle).getOozieClient());
    }
  }

  @Test
  public void open_delegateToFactoryUserPasswordArg_success() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      XOozieClient oozieClient = mock(XOozieClient.class);
      factory
          .when(() -> OozieClientFactory.createXOozieClient(eq(null), eq("admin"), eq("secret")))
          .thenReturn(oozieClient);

      // Act
      String args = "--connector oozie --user admin --password secret ";
      Handle handle = connector.open(toArgs(args));

      assertEquals(OozieHandle.class, handle.getClass());
      assertEquals(oozieClient, ((OozieHandle) handle).getOozieClient());
    }
  }

  private static ConnectorArguments toArgs(String args) throws Exception {
    return new ConnectorArguments(args.split(" "));
  }
}
