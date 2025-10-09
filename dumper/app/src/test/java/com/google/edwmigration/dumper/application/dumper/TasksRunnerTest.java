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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class TasksRunnerTest {
  @Rule
  public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Before
  public void setUp() {
    System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");  }

  @Test
  public void testCreateContext_returnsValidTaskRunContext() throws IOException {
    OutputHandleFactory mockSinkFactory = mock(OutputHandleFactory.class);
    Handle mockHandle = mock(Handle.class);
    int threadPoolSize = 2;
    TaskSetState.Impl mockState = mock(TaskSetState.Impl.class);
    ConnectorArguments arguments = new ConnectorArguments("--connector", "test");
    TasksRunner runner =
        new TasksRunner(
            mockSinkFactory,
            mockHandle,
            threadPoolSize,
            mockState,
            Collections.emptyList(),
            arguments);

    // Use reflection to access the private createContext method for direct testing
    try {
      java.lang.reflect.Method method =
          TasksRunner.class.getDeclaredMethod(
              "createContext",
              OutputHandleFactory.class,
              Handle.class,
              int.class,
              ConnectorArguments.class);
      method.setAccessible(true);
      Object context =
          method.invoke(runner, mockSinkFactory, mockHandle, threadPoolSize, arguments);

      assertNotNull(context);
      assertTrue(context instanceof TaskRunContext);

      TaskRunContext taskRunContext = (TaskRunContext) context;
      assertEquals(mockHandle, taskRunContext.getHandle());
      assertEquals(arguments, taskRunContext.getArguments());
    } catch (Exception e) {
      fail("Reflection failed: " + e.getMessage());
    }
  }

@Test
public void testGetTaskDuration_ReturnsMaxOfAllAndLatest() throws Exception {
  OutputHandleFactory mockSinkFactory = mock(OutputHandleFactory.class);
  Handle mockHandle = mock(Handle.class);
  int threadPoolSize = 2;
  TaskSetState.Impl mockState = mock(TaskSetState.Impl.class);
  ConnectorArguments arguments = new ConnectorArguments("--connector", "test");
  List<Task<?>> tasks = Collections.nCopies(10, mock(Task.class));
  TasksRunner runner =
      new TasksRunner(mockSinkFactory, mockHandle, threadPoolSize, mockState, tasks, arguments);

  // Set numberOfCompletedTasks to 5
  java.lang.reflect.Field completedField =
      TasksRunner.class.getDeclaredField("numberOfCompletedTasks");
  completedField.setAccessible(true);
  completedField.set(runner, new java.util.concurrent.atomic.AtomicInteger(5));

  // Mock stopwatch to control elapsed time
  java.lang.reflect.Field stopwatchField =
      TasksRunner.class.getDeclaredField("stopwatch");
  stopwatchField.setAccessible(true);
  com.google.common.base.Stopwatch mockStopwatch = mock(com.google.common.base.Stopwatch.class);
  when(mockStopwatch.elapsed()).thenReturn(Duration.ofSeconds(50));
  stopwatchField.set(runner, mockStopwatch);

  // Fill lastTaskDurations with 5 durations: 10, 20, 30, 40, 50 seconds
  java.lang.reflect.Field durationsField =
      TasksRunner.class.getDeclaredField("lastTaskDurations");
  durationsField.setAccessible(true);
  @SuppressWarnings("unchecked")
  Deque<Duration> durations = (Deque<Duration>) durationsField.get(runner);
  durations.clear();
  durations.add(Duration.ofSeconds(10));
  durations.add(Duration.ofSeconds(20));
  durations.add(Duration.ofSeconds(30));
  durations.add(Duration.ofSeconds(40));
  durations.add(Duration.ofSeconds(50));

  // getAverageTaskDurationFromAllTasks = 50s / 5 = 10s
  // getAverageTaskDurationFromLatestTasks = (50s - 10s) / 5 = 8s
  // getTaskDuration should return max(10s, 8s) = 10s
  java.lang.reflect.Method getTaskDuration = TasksRunner.class.getDeclaredMethod("getTaskDuration");
  getTaskDuration.setAccessible(true);
  Duration result = (Duration) getTaskDuration.invoke(runner);

  assertEquals(Duration.ofSeconds(10), result);
}

@Test
public void testGetTaskDuration_EmptyLastTaskDurations() throws Exception {
  OutputHandleFactory mockSinkFactory = mock(OutputHandleFactory.class);
  Handle mockHandle = mock(Handle.class);
  int threadPoolSize = 2;
  TaskSetState.Impl mockState = mock(TaskSetState.Impl.class);
  ConnectorArguments arguments = new ConnectorArguments("--connector", "test");
  List<Task<?>> tasks = Collections.nCopies(3, mock(Task.class));
  TasksRunner runner =
      new TasksRunner(mockSinkFactory, mockHandle, threadPoolSize, mockState, tasks, arguments);

  // Set numberOfCompletedTasks to 3
  java.lang.reflect.Field completedField =
      TasksRunner.class.getDeclaredField("numberOfCompletedTasks");
  completedField.setAccessible(true);
  completedField.set(runner, new java.util.concurrent.atomic.AtomicInteger(3));

  // Mock stopwatch to control elapsed time
  java.lang.reflect.Field stopwatchField =
      TasksRunner.class.getDeclaredField("stopwatch");
  stopwatchField.setAccessible(true);
  com.google.common.base.Stopwatch mockStopwatch = mock(com.google.common.base.Stopwatch.class);
  when(mockStopwatch.elapsed()).thenReturn(Duration.ofSeconds(9));
  stopwatchField.set(runner, mockStopwatch);

  // Ensure lastTaskDurations is empty
  java.lang.reflect.Field durationsField =
      TasksRunner.class.getDeclaredField("lastTaskDurations");
  durationsField.setAccessible(true);
  @SuppressWarnings("unchecked")
  Deque<Duration> durations = (Deque<Duration>) durationsField.get(runner);
  durations.clear();

  // getAverageTaskDurationFromAllTasks = 9s / 3 = 3s
  // getAverageTaskDurationFromLatestTasks = 0s
  // getTaskDuration should return max(3s, 0s) = 3s
  java.lang.reflect.Method getTaskDuration = TasksRunner.class.getDeclaredMethod("getTaskDuration");
  getTaskDuration.setAccessible(true);
  Duration result = (Duration) getTaskDuration.invoke(runner);

  assertEquals(Duration.ofSeconds(3), result);
}

@Test
public void testGetTaskDuration_LastTaskDurationsGreaterThanAllTasks() throws Exception {
  OutputHandleFactory mockSinkFactory = mock(OutputHandleFactory.class);
  Handle mockHandle = mock(Handle.class);
  int threadPoolSize = 2;
  TaskSetState.Impl mockState = mock(TaskSetState.Impl.class);
  ConnectorArguments arguments = new ConnectorArguments("--connector", "test");
  List<Task<?>> tasks = Collections.nCopies(4, mock(Task.class));
  TasksRunner runner =
      new TasksRunner(mockSinkFactory, mockHandle, threadPoolSize, mockState, tasks, arguments);

  // Set numberOfCompletedTasks to 2
  java.lang.reflect.Field completedField =
      TasksRunner.class.getDeclaredField("numberOfCompletedTasks");
  completedField.setAccessible(true);
  completedField.set(runner, new java.util.concurrent.atomic.AtomicInteger(2));

  // Mock stopwatch to control elapsed time
  java.lang.reflect.Field stopwatchField =
      TasksRunner.class.getDeclaredField("stopwatch");
  stopwatchField.setAccessible(true);
  com.google.common.base.Stopwatch mockStopwatch = mock(com.google.common.base.Stopwatch.class);
  when(mockStopwatch.elapsed()).thenReturn(Duration.ofSeconds(6));
  stopwatchField.set(runner, mockStopwatch);

  // Fill lastTaskDurations with 2 durations: 2s, 10s
  java.lang.reflect.Field durationsField =
      TasksRunner.class.getDeclaredField("lastTaskDurations");
  durationsField.setAccessible(true);
  @SuppressWarnings("unchecked")
  Deque<Duration> durations = (Deque<Duration>) durationsField.get(runner);
  durations.clear();
  durations.add(Duration.ofSeconds(2));
  durations.add(Duration.ofSeconds(10));

  // getAverageTaskDurationFromAllTasks = 6s / 2 = 3s
  // getAverageTaskDurationFromLatestTasks = (10s - 2s) / 2 = 4s
  // getTaskDuration should return max(3s, 4s) = 4s
  java.lang.reflect.Method getTaskDuration = TasksRunner.class.getDeclaredMethod("getTaskDuration");
  getTaskDuration.setAccessible(true);
  Duration result = (Duration) getTaskDuration.invoke(runner);

  assertEquals(Duration.ofSeconds(4), result);
}
}
