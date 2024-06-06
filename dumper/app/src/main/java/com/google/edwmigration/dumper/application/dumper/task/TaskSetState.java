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

  public static class TasksReport {

    private final long count;
    private final TaskState state;

    @Nonnull
    @Override
    public String toString() {
      return String.format("%d TASKS %s", count, state);
    }

    TasksReport(Long count, TaskState state) {
      this.count = count == null ? 0 : count;
      this.state = state;
    }
  }

  @ThreadSafe
  public static class Impl implements TaskSetState {

    @GuardedBy("lock")
    private final Map<Task<?>, TaskResult<?>> resultMap = new HashMap<>();

    private final Object lock = new Object();

    public long failedRequiredTaskCount() {
      synchronized (lock) {
        return resultMap.entrySet().stream()
            .filter(e -> TaskCategory.REQUIRED.equals(e.getKey().getCategory()))
            .filter(e -> TaskState.FAILED.equals(e.getValue().getState()))
            .count();
      }
    }

    public ImmutableList<TaskResultSummary> taskResultSummaries() {
      synchronized (lock) {
        ImmutableList.Builder<TaskResultSummary> builder = ImmutableList.builder();
        resultMap.forEach(
            (task, result) -> {
              TaskResultSummary summary = TaskResultSummary.create(task, result);
              builder.add(summary);
            });
        return builder.build();
      }
    }

    public ImmutableList<TasksReport> tasksReports() {
      synchronized (lock) {
        ImmutableList.Builder<TasksReport> builder = ImmutableList.builder();
        resultMap.values().stream()
            .collect(Collectors.groupingBy(TaskResult::getState, Collectors.counting()))
            .forEach((key, value) -> builder.add(new TasksReport(value, key)));
        return builder.build();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> TaskResult<T> getTaskResult(@Nonnull Task<T> task) {
      synchronized (lock) {
        return (TaskResult<T>) resultMap.get(task);
      }
    }

    public <T> void setTaskResult(
        @Nonnull Task<T> task, @Nonnull TaskState state, @CheckForNull T value) {
      synchronized (lock) {
        resultMap.put(task, new TaskResult<>(state, value));
      }
    }

    public <T> void setTaskException(
        @Nonnull Task<T> task, @Nonnull TaskState state, @CheckForNull Exception exception) {
      synchronized (lock) {
        resultMap.put(task, new TaskResult<>(state, exception));
      }
    }
  }

  @CheckForNull
  public <T> TaskResult<T> getTaskResult(@Nonnull Task<T> task);

  @Nonnull
  public default TaskState getTaskState(@Nonnull Task<?> task) {
    TaskResult<?> result = getTaskResult(task);
    return (result == null) ? TaskState.NOT_STARTED : result.getState();
  }
}
