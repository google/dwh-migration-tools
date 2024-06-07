/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.task;

import static com.google.edwmigration.dumper.application.dumper.task.TaskCategory.REQUIRED;
import static com.google.edwmigration.dumper.application.dumper.task.TaskState.FAILED;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/** @author shevek */
public interface TaskSetState {

  @ParametersAreNonnullByDefault
  static class TaskResultSummary {
    private final String exception;
    private final Task<?> task;
    private final TaskState state;

    private TaskResultSummary(String exception, Task<?> task, TaskState state) {
      this.exception = exception;
      this.task = task;
      this.state = state;
    }

    @Nonnull
    static TaskResultSummary create(Task<?> task, TaskResult<?> result) {
      String optionalException = String.format(": %s", result.getException());
      String exceptionDescription = result.getException() == null ? "" : optionalException;
      return new TaskResultSummary(exceptionDescription, task, result.getState());
    }

    @Nonnull
    @Override
    public String toString() {
      return String.format("Task %s (%s) %s%s", state, task.getCategory(), task, exception);
    }
  }

  @ParametersAreNonnullByDefault
  public static class TasksReport {

    private final long count;
    private final TaskState state;

    @Nonnull
    @Override
    public String toString() {
      return String.format("%d TASKS %s", count, state);
    }

    TasksReport(long count, TaskState state) {
      this.count = count;
      this.state = state;
    }
  }

  @ThreadSafe
  public static class Impl implements TaskSetState {

    @GuardedBy("this")
    private final Map<Task<?>, TaskResult<?>> resultMap = new HashMap<>();

    @Deprecated // Use TaskSetState instead of TaskSetState.Impl
    @Override
    public synchronized long failedRequiredTaskCount() {
      long result = 0;
      for (Task<?> key : resultMap.keySet()) {
        TaskState state = resultMap.get(key).getState();
        if (key.getCategory() == REQUIRED && state == FAILED) {
          result++;
        }
      }
      return result;
    }

    @Deprecated // Use TaskSetState instead of TaskSetState.Impl
    @Nonnull
    @Override
    public synchronized ImmutableList<TaskResultSummary> taskResultSummaries() {
      ImmutableList.Builder<TaskResultSummary> builder = ImmutableList.builder();
      resultMap.forEach(
          (task, result) -> {
            TaskResultSummary summary = TaskResultSummary.create(task, result);
            builder.add(summary);
          });
      return builder.build();
    }

    @Deprecated // Use TaskSetState instead of TaskSetState.Impl
    @Nonnull
    @Override
    public synchronized ImmutableList<TasksReport> tasksReports() {
      ImmutableList.Builder<TasksReport> builder = ImmutableList.builder();
      resultMap.values().stream()
          .collect(Collectors.groupingBy(TaskResult::getState, Collectors.counting()))
          .forEach((key, value) -> builder.add(new TasksReport(value, key)));
      return builder.build();
    }

    @Nonnull
    @Override
    public TaskState getTaskState(@Nonnull Task<?> task) {
      TaskResult<?> result = resultMap.get(task);
      return (result == null) ? TaskState.NOT_STARTED : result.getState();
    }

    public synchronized <T> void setTaskResult(
        @Nonnull Task<T> task, @Nonnull TaskState state, @CheckForNull T value) {
      resultMap.put(task, new TaskResult<>(state, value));
    }

    public synchronized void setTaskException(
        @Nonnull Task<?> task, @Nonnull TaskState state, @CheckForNull Exception exception) {
      resultMap.put(task, new TaskResult<>(state, exception));
    }
  }

  long failedRequiredTaskCount();

  @Nonnull
  ImmutableList<TaskResultSummary> taskResultSummaries();

  @Nonnull
  ImmutableList<TasksReport> tasksReports();

  @Nonnull
  TaskState getTaskState(@Nonnull Task<?> task);
}
