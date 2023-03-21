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
package com.google.edwmigration.dumper.application.dumper.task;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/** @author shevek */
public class TaskGroup extends AbstractTask<Void> implements CoreMetadataDumpFormat {

  private final List<Task<?>> tasks = new ArrayList<>();

  public TaskGroup(@Nonnull String targetPath, @Nonnull Task<?>... tasks) {
    super(targetPath);
    Collections.addAll(this.tasks, tasks);
  }

  public void addTask(Task<?> task) {
    // Checking for conditions would need some ordering of tasks execution or waiting on {@link
    // TaskSetState#getTaskResult}
    // Preconditions.checkState(task.getConditions().length == 0, "Tasks in a parallel task should
    // not have conditions");
    // Preconditions.checkState(task instanceof AbstractJdbcSelectTask || task instanceof
    // FormatTask, "Parallel task only supports JdbcSelectTask and FormatTask sub tasks. Trying to
    // add %s.", task.getClass().getSimpleName());
    tasks.add(task);
  }

  @Nonnull
  public List<Task<?>> getTasks() {
    return tasks;
  }

  protected void doRun(
      @Nonnull TaskRunContext context, @Nonnull CSVPrinter printer, @Nonnull Handle handle)
      throws Exception {
    for (Task<?> task : tasks) {
      context.runChildTask(task);
      TaskState state = context.getTaskState(task);
      printer.printRecord(task, state);
    }
  }

  @Override
  protected final Void doRun(TaskRunContext context, ByteSink sink, Handle handle)
      throws Exception {
    CSVFormat format = FORMAT.withHeader(CoreMetadataDumpFormat.Group.Header.class);
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = format.print(writer)) {
      doRun(context, printer, handle);
    }
    return null;
  }

  @Override
  public String toString() {
    return "TaskGroup(" + getTasks().size() + " children)";
  }
}
