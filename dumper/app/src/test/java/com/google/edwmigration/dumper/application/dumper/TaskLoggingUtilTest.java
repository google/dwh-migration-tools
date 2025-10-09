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
import static org.mockito.Mockito.*;

import com.google.common.base.Stopwatch;
import java.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TaskLoggingUtilTest {

  private TaskLoggingUtil util;

  @Before
  public void setUp() {
    util = new TaskLoggingUtil(5);
  }

  @Test
  public void testRecordTaskCompletion_IncrementsCompletedTasksAndStoresDurations() {
    int initialCompleted = util.numberOfCompletedTasks.get();
    util.recordTaskCompletion();
    assertEquals(initialCompleted + 1, util.numberOfCompletedTasks.get());
    // After first completion, lastTaskDurations should have 1 element
    assertEquals(1, util.lastTaskDurations.size());
    util.recordTaskCompletion();
    assertEquals(2, util.lastTaskDurations.size());
  }

  @Test
  public void testRecordTaskCompletion_QueueDoesNotExceedMaxSize() {
    for (int i = 0; i < 15; i++) {
      util.recordTaskCompletion();
    }
    // Should not exceed TASK_QUEUE_SIZE (10)
    assertEquals(10, util.lastTaskDurations.size());
  }

  @Test
  public void testGetProgressLog_ETA_NotShownWhenCompletedTasksNotEnough() {
    // Complete less than or equal to TASK_QUEUE_SIZE tasks
    for (int i = 0; i < 10; i++) {
      util.recordTaskCompletion();
    }
    Stopwatch spyStopwatch = Mockito.spy(Stopwatch.createUnstarted());
    spyStopwatch.start();
    spyStopwatch.reset();
    when(spyStopwatch.elapsed()).thenReturn(Duration.ofMinutes(16));
    try {
      java.lang.reflect.Field field = TaskLoggingUtil.class.getDeclaredField("stopwatch");
      field.setAccessible(true);
      field.set(util, spyStopwatch);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    String log = util.getProgressLog();
    // ETA should not be shown since completedTasks <= TASK_QUEUE_SIZE
    assertEquals(false, log.contains("ETA"));
  }

  @Test
  public void testGetProgressLog_RemainingDurationFromAllTasksWhenLatestTasksEmpty() {
    // Only one task completed, so lastTaskDurations has one element
    util.recordTaskCompletion();
    Stopwatch spyStopwatch = Mockito.spy(Stopwatch.createUnstarted());
    spyStopwatch.start();
    spyStopwatch.reset();
    when(spyStopwatch.elapsed()).thenReturn(Duration.ofMinutes(15));
    try {
      java.lang.reflect.Field field = TaskLoggingUtil.class.getDeclaredField("stopwatch");
      field.setAccessible(true);
      field.set(util, spyStopwatch);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    String log = util.getProgressLog();
    // Should not show ETA, and should use all-tasks average
    assertEquals(false, log.contains("ETA"));
    assertEquals(true, log.contains("% Completed"));
  }

  @Test
  public void testRecordTaskCompletion_HandlesMultipleWraps() {
    // Fill up the queue, then add more to ensure oldest are removed
    for (int i = 0; i < 25; i++) {
      util.recordTaskCompletion();
    }
    assertEquals(10, util.lastTaskDurations.size());
  }

  @Test
  public void testGetProgressLog_ZeroTasks() {
    // No tasks completed
    String log = util.getProgressLog();
    assertEquals(true, log.contains("0% Completed"));
    assertEquals(false, log.contains("ETA"));
  }

  @Test
  public void testGetProgressLog_UsesAllTasksAverage() {
    // Simulate only 1 completion, so latestTasks is empty or not enough
    util.recordTaskCompletion();
    String log = util.getProgressLog();
    // Should not contain ETA since completedTasks <= TASK_QUEUE_SIZE
    assertEquals(false, log.contains("ETA"));
    // Should show percent completed
    assertEquals(true, log.contains("% Completed"));
  }

  @Test
  public void testGetProgressLog_LessThanTaskQueueSize() {
    // Complete less than TASK_QUEUE_SIZE tasks
    for (int i = 0; i < 5; i++) {
      util.recordTaskCompletion();
    }
    String log = util.getProgressLog();
    // Should not contain ETA
    assertEquals(false, log.contains("ETA"));
    // Should show percent completed
    assertEquals(true, log.contains("% Completed"));
  }
}
