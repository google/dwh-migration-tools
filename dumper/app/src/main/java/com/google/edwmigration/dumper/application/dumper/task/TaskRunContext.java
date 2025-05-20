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
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.plugin.ext.jdk.concurrent.ExecutorManager;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;

/** @author shevek */
public class TaskRunContext implements OutputHandleFactory {

  private final OutputHandleFactory sinkFactory;
  private final Handle handle;
  // Should we not create a pool here but instead just pass the int around, so a task can make a
  // backpressure or non-backpressure pool appropriately?
  private final Executor executorService;
  private final TaskRunContextOps ops;

  private final Path targetDirectory;
  private final ConnectorArguments arguments;

  public TaskRunContext(
      OutputHandleFactory sinkFactory,
      Handle handle,
      int threadPoolSize,
      TaskRunContextOps ops,
      ConnectorArguments arguments) {
    this(
        sinkFactory,
        handle,
        ExecutorManager.newUnboundedExecutorService("task-run-context", threadPoolSize),
        ops,
        Paths.get(""),
        arguments);
  }

  private TaskRunContext(
      OutputHandleFactory sinkFactory,
      Handle handle,
      Executor executorService,
      TaskRunContextOps ops,
      Path targetDirectory,
      ConnectorArguments arguments) {
    this.sinkFactory = Preconditions.checkNotNull(sinkFactory, "ByteSinkFactory was null.");
    this.handle = Preconditions.checkNotNull(handle, "Handle was null.");
    this.executorService = executorService;
    this.ops = ops;
    this.targetDirectory = targetDirectory;
    this.arguments = Preconditions.checkNotNull(arguments, "Arguments was null.");
  }

  @Nonnull
  public Handle getHandle() {
    return handle;
  }

  @Nonnull
  public TaskRunContext forChildConnector(
      Handle handle, Path targetDirectory, ConnectorArguments arguments) {
    return new TaskRunContext(
        sinkFactory,
        handle,
        executorService,
        ops,
        this.targetDirectory.resolve(targetDirectory),
        arguments);
  }

  @Override
  public OutputHandle newOutputFileHandle(String targetPath) {
    return sinkFactory.newOutputFileHandle(targetDirectory.resolve(targetPath).toString());
  }

  public OutputHandle createOutputHandle(String targetPath) throws IOException {
    OutputHandle outputHandle = newOutputFileHandle(targetPath);
    if (outputHandle.exists()) {
      throw new IllegalStateException(
          String.format("Attempt to create two sinks for the same target path='%s'.", targetPath));
    }
    return outputHandle;
  }

  @Nonnull
  public ConnectorArguments getArguments() {
    return arguments;
  }

  @Nonnull
  public TaskState getTaskState(@Nonnull Task<?> task) {
    return ops.getTaskState(task);
  }

  // Only used by ParallelTaskGroup at the moment.
  @Nonnull
  public Executor getExecutorService() {
    return executorService;
  }

  /** nothrow */
  public <T> T runChildTask(@Nonnull Task<T> task) throws MetadataDumperUsageException {
    return ops.runChildTask(task);
  }
}
