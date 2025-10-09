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

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle.WriteMode;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskGroup;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContextOps;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import com.google.edwmigration.dumper.application.dumper.task.TaskState;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author ishmum */
public class TasksRunner implements TaskRunContextOps {

  private static final Logger logger = LoggerFactory.getLogger(TasksRunner.class);
  public static final Logger PROGRESS_LOG = LoggerFactory.getLogger("progress-logger");

  private AtomicInteger numberOfCompletedTasks;
  private final int totalNumberOfTasks;
  private final Stopwatch stopwatch;

  private final TaskRunContext context;
  private final TaskSetState.Impl state;
  private final List<Task<?>> tasks;

  // For tracking last 10 finished task durations
  private final Deque<Duration> lastTaskDurations = new ArrayDeque<>(10);

  public TasksRunner(
      OutputHandleFactory sinkFactory,
      Handle handle,
      int threadPoolSize,
      @Nonnull TaskSetState.Impl state,
      List<Task<?>> tasks,
      ConnectorArguments arguments) {
    context = createContext(sinkFactory, handle, threadPoolSize, arguments);
    this.state = state;
    this.tasks = tasks;
    totalNumberOfTasks = countTasks(tasks);
    stopwatch = Stopwatch.createStarted();
    numberOfCompletedTasks = new AtomicInteger();
  }

  private TaskRunContext createContext(
      OutputHandleFactory sinkFactory,
      Handle handle,
      int threadPoolSize,
      ConnectorArguments arguments) {
    return new TaskRunContext(sinkFactory, handle, threadPoolSize, this, arguments);
  }

  @Nonnull
  @Override
  public TaskState getTaskState(@Nonnull Task<?> task) {
    return state.getTaskState(task);
  }

  @Override
  public <T> T runChildTask(@Nonnull Task<T> task) throws MetadataDumperUsageException {
    return handleTask(task);
  }

  public void run() throws MetadataDumperUsageException {
    for (Task<?> task : tasks) {
      handleTask(task);
    }
  }

  @CheckForNull
  private <T> T handleTask(Task<T> task) throws MetadataDumperUsageException {
    T t = runTask(task);
    if (!(task instanceof TaskGroup)) {
      numberOfCompletedTasks.getAndIncrement();

      Duration taskDuration = stopwatch.elapsed();
      if (lastTaskDurations.size() == 10) {
        lastTaskDurations.removeFirst();
      }
      lastTaskDurations.addLast(taskDuration);
    }
    logProgress();
    return t;
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

  private void logProgress() {
    int numberOfCompletedTasks = this.numberOfCompletedTasks.get();

    Duration averageTimePerTask = getTaskDuration();

    int percentFinished = numberOfCompletedTasks * 100 / totalNumberOfTasks;
    String progressMessage = percentFinished + "% Completed";

    int remainingTasks = totalNumberOfTasks - numberOfCompletedTasks;
    Duration remainingTime = averageTimePerTask.multipliedBy(remainingTasks);

    if (numberOfCompletedTasks > 10 && remainingTasks > 0) {
      progressMessage += ". ETA: " + formatApproximateDuration(remainingTime);
    }

    PROGRESS_LOG.info(progressMessage);
  }

  private static final String ACCESS_CONTROL_EXCEPTION_MSG_SUFFIX =
      ".AccessControlException: SIMPLE authentication is not enabled.  Available:[TOKEN, KERBEROS]";

  private <T> T runTask(Task<T> task) throws MetadataDumperUsageException {
    try {
      CHECK:
      {
        TaskState ts = state.getTaskState(task);
        Preconditions.checkState(
            ts == TaskState.NOT_STARTED, "TaskState was bad: " + ts + " for " + task);
      }

      PRECONDITION:
      for (Task.Condition condition : task.getConditions()) {
        if (!condition.evaluate(state)) {
          logger.debug("Skipped " + task.getName() + " because " + condition.toSkipReason());
          state.setTaskResult(task, TaskState.SKIPPED, null);
          return null;
        }
      }

      RUN:
      {
        T value = task.run(context);
        state.setTaskResult(task, TaskState.SUCCEEDED, value);
        return value;
      }
    } catch (Exception e) {
      // MetadataDumperUsageException should be fatal.
      if (e instanceof MetadataDumperUsageException) throw (MetadataDumperUsageException) e;
      if (e instanceof SQLException && e.getCause() instanceof MetadataDumperUsageException)
        throw (MetadataDumperUsageException) e.getCause();
      if (e instanceof IOException // is it a org.apache.hadoop.security.AccessControlException?
          && e.getMessage().endsWith(ACCESS_CONTROL_EXCEPTION_MSG_SUFFIX)) {
        if (!task.handleException(e))
          logger.warn("Task failed due to access denied: {}: {}", task, e.getMessage());
      }
      // TaskGroup is an attempt to get rid of this condition.
      // We might need an additional TaskRunner / TaskSupport with an overrideable handleException
      // method instead of this runTask() method.
      else if (!task.handleException(e))
        logger.warn("Task failed: {}: {}", task, e.getMessage(), e);
      state.setTaskException(task, TaskState.FAILED, e);
      try {
        OutputHandle sink = context.newOutputFileHandle(task.getTargetPath() + ".exception.txt");
        sink.asCharSink(StandardCharsets.UTF_8, WriteMode.CREATE_TRUNCATE)
            .writeLines(
                Arrays.asList(
                    task.toString(),
                    "******************************",
                    String.valueOf(new DumperDiagnosticQuery(e).call())));
      } catch (Exception f) {
        logger.warn("Exception-recorder failed:  {}", f.getMessage(), f);
      }
    }
    return null;
  }

  private int countTasks(List<Task<?>> tasks) {
    return tasks.stream()
        .mapToInt(task -> task instanceof TaskGroup ? countTasks(((TaskGroup) task).getTasks()) : 1)
        .sum();
  }
}
