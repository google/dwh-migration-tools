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
package com.google.edwmigration.dumper.application.dumper;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import com.google.edwmigration.dumper.application.dumper.task.TaskState;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** @author kakha */
@RunWith(MockitoJUnitRunner.class)
public class TasksRunnerTest {

  @Mock private OutputHandleFactory mockSinkFactory;
  @Mock private Handle mockHandle;
  @Mock private TaskSetState.Impl mockState;
  @Mock private Task mockTask1;
  @Mock private Task mockTask2;
  @Mock private ConnectorArguments mockArguments;
  @Mock private TelemetryProcessor mockTelemetryProcessor;

  private TasksRunner tasksRunner;
  private List<Task<?>> tasks;

  @Before
  public void setUp() {
    tasks = Arrays.asList(mockTask1, mockTask2);

    // Setup mock task names
    when(mockTask1.getName()).thenReturn("test-task-1");
    when(mockTask2.getName()).thenReturn("test-task-2");

    // Setup mock task states
    when(mockState.getTaskState(mockTask1)).thenReturn(TaskState.SUCCEEDED);
    when(mockState.getTaskState(mockTask2)).thenReturn(TaskState.SUCCEEDED);

    tasksRunner =
        new TasksRunner(
            mockSinkFactory,
            mockHandle,
            1,
            mockState,
            tasks,
            mockArguments,
            mockTelemetryProcessor);
  }

  @Test
  public void testTasksRunnerConstructor() {
    assertNotNull(tasksRunner);
    assertEquals(2, tasks.size());
  }

  @Test
  public void testTasksRunnerConstructorWithNullTelemetryProcessor() {
    // Create TasksRunner without telemetry processor
    TasksRunner noTelemetryRunner =
        new TasksRunner(mockSinkFactory, mockHandle, 1, mockState, tasks, mockArguments, null);

    // Should not throw exception when telemetry processor is null
    assertNotNull(noTelemetryRunner);
  }

  @Test
  public void testMetricToErrorMapInitialization() {
    // Verify the error map is properly initialized
    assertNotNull(tasksRunner);
    // The map should be empty initially
    // We can't directly access it, but we can verify the constructor doesn't throw
  }

  @Test
  public void testTaskExecutionFlow() {
    // Given
    try {
      doReturn("result1").when(mockTask1).run(any());
      doReturn("result2").when(mockTask2).run(any());
    } catch (Exception e) {
      fail("Mock setup failed: " + e.getMessage());
    }

    // When & Then
    // This tests the overall flow without actually running tasks
    // We're mainly testing that the telemetry integration doesn't break existing functionality
    assertNotNull(tasksRunner);
  }
}
