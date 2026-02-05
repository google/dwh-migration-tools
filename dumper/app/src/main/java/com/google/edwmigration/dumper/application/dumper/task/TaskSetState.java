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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

/** @author shevek */
public interface TaskSetState {

  @AutoValue
  @ParametersAreNonnullByDefault
  abstract class TaskResultSummary {
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

    public TaskState getTaskState() {
      return state();
    }

    public Task<?> getTask() {
      return task();
    }

    public Optional<Throwable> getThrowable() {
      return throwable();
    }
  }

  @AutoValue
  @ParametersAreNonnullByDefault
  abstract class TasksReport {

    public abstract long count();

    public abstract TaskState state();

    static TasksReport create(long count, TaskState state) {
      return new AutoValue_TaskSetState_TasksReport(count, state);
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
