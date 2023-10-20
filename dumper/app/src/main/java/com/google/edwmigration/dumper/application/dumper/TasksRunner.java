/*
 * Copyright 2022-2023 Google LLC
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

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskGroup;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState.Impl;
import com.google.edwmigration.dumper.application.dumper.task.TaskState;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author ishmum */
public class TasksRunner {

  private static final Logger LOG = LoggerFactory.getLogger(TasksRunner.class);
  private static final Logger PROGRESS_LOG = LoggerFactory.getLogger("progress-logger");

  private AtomicInteger numberOfCompletedTasks;
  private final int totalNumberOfTasks;
  private final Stopwatch stopwatch;

  private final TaskRunContext context;
  private final TaskSetState.Impl state;
  private final List<Task<?>> tasks;

  public TasksRunner(
      OutputHandleFactory sinkFactory,
      Handle handle,
      int threadPoolSize,
      @Nonnull TaskSetState.Impl state,
      List<Task<?>> tasks) {
    context = createContext(sinkFactory, handle, threadPoolSize, state);
    this.state = state;
    this.tasks = tasks;
    totalNumberOfTasks = countTasks(tasks);
    stopwatch = Stopwatch.createStarted();
    numberOfCompletedTasks = new AtomicInteger();
  }

  private TaskRunContext createContext(
      OutputHandleFactory sinkFactory, Handle handle, int threadPoolSize, Impl state) {
    return new TaskRunContext(sinkFactory, handle, threadPoolSize) {
      @Override
      public TaskState getTaskState(Task<?> task) {
        return state.getTaskState(task);
      }

      @Override
      public <T> T runChildTask(Task<T> task) throws MetadataDumperUsageException {
        return handleTask(task);
      }
    };
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
    }
    logProgress();
    return t;
  }

  private void logProgress() {
    int numberOfCompletedTasks = this.numberOfCompletedTasks.get();

    long averageTimePerTaskInMillisecond =
        numberOfCompletedTasks == 0
            ? 0
            : stopwatch.elapsed(TimeUnit.MILLISECONDS) / numberOfCompletedTasks;

    int percentFinished = numberOfCompletedTasks * 100 / totalNumberOfTasks;
    String progressMessage = percentFinished + "% Completed";

    int remainingTasks = totalNumberOfTasks - numberOfCompletedTasks;
    long remainingTimeInMillis = averageTimePerTaskInMillisecond * remainingTasks;

    if (numberOfCompletedTasks > 10 && remainingTasks > 0) {
      progressMessage += ". ETC: ~" + formatDuration(remainingTimeInMillis);
    }

    PROGRESS_LOG.info(progressMessage);
  }

  private String formatDuration(long durationMillis) {
    String duration = DurationFormatUtils.formatDuration(durationMillis, "''H' hours 'm' minutes'");
    duration = StringUtils.replaceOnce(duration, "0 hours 0 minutes", "less than one minute");
    duration = StringUtils.replaceOnce(duration, "0 hours ", StringUtils.EMPTY);
    duration = StringUtils.replaceOnce(duration, " 0 minutes", StringUtils.EMPTY);
    duration = StringUtils.replaceOnce(duration, "1 hours", "1 hour");
    duration = StringUtils.replaceOnce(duration, "1 minutes", "1 minute");
    return duration;
  }

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
          LOG.debug("Skipped " + task.getName() + " because " + condition.toSkipReason());
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
      // TaskGroup is an attempt to get rid of this condition.
      // We might need an additional TaskRunner / TaskSupport with an overrideable handleException
      // method instead of this runTask() method.
      if (!task.handleException(e)) LOG.warn("Task failed: " + task + ": " + e, e);
      state.setTaskException(task, TaskState.FAILED, e);
      try {
        OutputHandle sink = context.newOutputFileHandle(task.getTargetPath() + ".exception.txt");
        sink.asCharSink(StandardCharsets.UTF_8)
            .writeLines(
                Arrays.asList(
                    task.toString(),
                    "******************************",
                    String.valueOf(new DumperDiagnosticQuery(e).call())));
      } catch (Exception f) {
        LOG.warn("Exception-recorder failed:  " + f, f);
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
