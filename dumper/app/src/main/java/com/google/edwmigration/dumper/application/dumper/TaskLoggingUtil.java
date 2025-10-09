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

import static com.google.edwmigration.dumper.application.dumper.DurationFormatter.formatApproximateDuration;
import static java.lang.Math.max;

import com.google.common.base.Stopwatch;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;

class TaskLoggingUtil {
  private final int TASK_QUEUE_SIZE = 10;
  AtomicInteger numberOfCompletedTasks;
  final int totalNumberOfTasks;
  final Stopwatch stopwatch;
  final Deque<Duration> lastTaskDurations = new ArrayDeque<>(TASK_QUEUE_SIZE);

  public TaskLoggingUtil(int totalNumberOfTasks) {
    this.totalNumberOfTasks = totalNumberOfTasks;
    stopwatch = Stopwatch.createStarted();
    numberOfCompletedTasks = new AtomicInteger();
  }

  private Duration getAverageTaskDurationFromAllTasks() {
    return stopwatch.elapsed().dividedBy(max(1, numberOfCompletedTasks.get()));
  }

  private Duration getAverageTaskDurationFromLatestTasks() {
    if (lastTaskDurations.isEmpty()) {
      return Duration.ZERO;
    }
    Duration total = lastTaskDurations.getLast().minus(lastTaskDurations.getFirst());

    return total.dividedBy(lastTaskDurations.size());
  }

  private Duration getTaskDuration() {
    return Collections.max(
        Arrays.asList(
            getAverageTaskDurationFromAllTasks(), getAverageTaskDurationFromLatestTasks()));
  }

  public void recordTaskCompletion() {
    numberOfCompletedTasks.getAndIncrement();

    Duration taskDuration = stopwatch.elapsed();
    if (lastTaskDurations.size() == TASK_QUEUE_SIZE) {
      lastTaskDurations.removeFirst();
    }
    lastTaskDurations.addLast(taskDuration);
  }

  public String getProgressLog() {
    int completedTasks = numberOfCompletedTasks.get();

    Duration averageTimePerTask = getTaskDuration();

    int percentFinished = completedTasks * 100 / totalNumberOfTasks;
    String progressMessage = percentFinished + "% Completed";

    int remainingTasks = totalNumberOfTasks - completedTasks;
    Duration remainingTime = averageTimePerTask.multipliedBy(remainingTasks);

    if (completedTasks > TASK_QUEUE_SIZE && remainingTasks > 0) {
      progressMessage += ". ETA: " + formatApproximateDuration(remainingTime);
    }

    return progressMessage;
  }
}
