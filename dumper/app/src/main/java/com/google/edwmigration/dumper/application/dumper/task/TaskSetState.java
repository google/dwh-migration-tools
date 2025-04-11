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
package com.google.edwmigration.dumper.application.dumper.task;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.edwmigration.dumper.application.dumper.task.TaskCategory.REQUIRED;
import static com.google.edwmigration.dumper.application.dumper.task.TaskState.FAILED;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/** @author shevek */
public interface TaskSetState {

  @AutoValue
  @ParametersAreNonnullByDefault
  abstract static class TaskResultSummary {
    abstract Optional<Throwable> throwable();

    abstract Task<?> task();

    abstract TaskState state();

    @Nonnull
    static TaskResultSummary create(Task<?> task, TaskResult<?> result) {
      Optional<Throwable> throwable = Optional.ofNullable(result.getException());
      return new AutoValue_TaskSetState_TaskResultSummary(throwable, task, result.getState());
    }

    @Nonnull
    @Override
    public String toString() {
      String prefix = String.format("Task %s (%s) %s", state(), task().getCategory(), task());
      StringBuilder buf = new StringBuilder(prefix);
      if (throwable().isPresent()) {
        buf.append(": ").append(throwable().get());
      }
      return buf.toString();
    }
  }

  @AutoValue
  @ParametersAreNonnullByDefault
  abstract static class TasksReport {

    public abstract long count();

    public abstract TaskState state();

    static TasksReport create(long count, TaskState state) {
      return new AutoValue_TaskSetState_TasksReport(count, state);
    }
  }

  @ThreadSafe
  static class Impl implements TaskSetState {

    // The map is used for user-visible reporting, using an insertion-ordered map will make
    // reporting consistent.
    @GuardedBy("this")
    private final Map<Task<?>, TaskResult<?>> resultMap = new LinkedHashMap<>();

    @Deprecated // Use TaskSetState instead of TaskSetState.Impl
    @Override
    public synchronized long getFailedRequiredTaskCount() {
      long result = 0;
      for (Map.Entry<Task<?>, TaskResult<?>> entry : resultMap.entrySet()) {
        TaskState state = entry.getValue().getState();
        if (entry.getKey().getCategory() == REQUIRED && state == FAILED) {
          result++;
        }
      }
      return result;
    }

    @Deprecated // Use TaskSetState instead of TaskSetState.Impl
    @Nonnull
    @Override
    public synchronized ImmutableList<TaskResultSummary> getTaskResultSummaries() {
      return resultMap.entrySet().stream()
          .map(entry -> TaskResultSummary.create(entry.getKey(), entry.getValue()))
          .collect(toImmutableList());
    }

    @Deprecated // Use TaskSetState instead of TaskSetState.Impl
    @Nonnull
    @Override
    public synchronized ImmutableList<TasksReport> getTasksReports() {
      return resultMap.values().stream()
          .collect(groupingBy(TaskResult::getState, counting()))
          .entrySet()
          .stream()
          .map(entry -> TasksReport.create(entry.getValue(), entry.getKey()))
          .collect(toImmutableList());
    }

    @Nonnull
    @Override
    public synchronized TaskState getTaskState(@Nonnull Task<?> task) {
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

  long getFailedRequiredTaskCount();

  @Nonnull
  ImmutableList<TaskResultSummary> getTaskResultSummaries();

  @Nonnull
  ImmutableList<TasksReport> getTasksReports();

  @Nonnull
  TaskState getTaskState(@Nonnull Task<?> task);
}
