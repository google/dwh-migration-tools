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

import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.plugin.ext.jdk.concurrent.ExecutorManager;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.csv.CSVPrinter;

public class ParallelTaskGroup extends TaskGroup {

  public ParallelTaskGroup(String name) {
    super("parallel-task-" + name);
  }

  ParallelTaskGroup(String name, List<Task<?>> tasks) {
    super("parallel-task-" + name, tasks);
  }

  @Override
  public void addTask(Task<?> task) {
    // Checking for conditions would need some ordering of tasks execution or waiting on {@link
    // TaskSetState#getTaskResult}
    Preconditions.checkState(
        task.getConditions().length == 0, "Tasks in a parallel task should not have conditions");
    Preconditions.checkState(
        task instanceof AbstractJdbcTask || task instanceof FormatTask,
        "Parallel task only supports JdbcSelectTask and FormatTask sub tasks. Trying to add %s.",
        task.getClass().getSimpleName());
    super.addTask(task);
  }

  private static class TaskRunner {

    @Nonnull private final TaskRunContext context;
    @Nonnull private final Task<?> task;
    @Nonnull private final CSVPrinter printer;

    public TaskRunner(
        @Nonnull TaskRunContext context, @Nonnull Task<?> task, @Nonnull CSVPrinter printer) {
      this.context = context;
      this.task = task;
      this.printer = printer;
    }

    public @Nullable Object call() throws IOException {
      Object result = context.runChildTask(task);
      TaskState state = context.getTaskState(task);
      synchronized (printer) {
        printer.printRecord(task, state);
      }
      return result;
    }
  }

  @Override
  protected void doRun(
      @Nonnull TaskRunContext context, @Nonnull CSVPrinter printer, @Nonnull Handle handle)
      throws ExecutionException, InterruptedException {
    // Throws ExecutionException if any sub-task threw. However, runChildTask() is nothrow, so that
    // never happens.
    // We safely publish the CSVPrinter to the ExecutorManager.
    try (ExecutorManager executorManager = new ExecutorManager(context.getExecutorService())) {
      for (Task<?> task : getTasks()) {
        TaskRunner runner = new TaskRunner(context, task, printer);
        executorManager.execute(runner::call);
      }
    }
    // We now, by the t-w-r, safely collect the CSVPrinter from the sub-threads.
  }

  @Override
  public String toString() {
    return "ParallelTaskGroup(" + getTasks().size() + " children)";
  }

  public static class Builder {

    private final ArrayList<Task<?>> taskList = new ArrayList<>();
    private final String name;

    public Builder(String name) {
      this.name = name;
    }

    @CanIgnoreReturnValue
    public Builder addTask(Task<?> task) {
      Preconditions.checkState(
          task.getConditions().length == 0, "Tasks in a parallel task should not have conditions");
      Preconditions.checkState(
          task instanceof AbstractJdbcTask || task instanceof FormatTask,
          "Parallel task only supports JdbcSelectTask and FormatTask sub tasks. Trying to add %s.",
          task.getClass().getSimpleName());
      taskList.add(task);
      return this;
    }

    public ParallelTaskGroup build() {
      return new ParallelTaskGroup(name, taskList);
    }
  }
}
