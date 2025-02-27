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
package com.google.edwmigration.dumper.application.dumper.test;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContextOps;
import com.google.edwmigration.dumper.application.dumper.task.TaskState;
import java.io.IOException;
import java.io.UncheckedIOException;
import javax.annotation.Nonnull;

/** @author shevek */
public class DummyTaskRunContextFactory {

  public static TaskRunContext create(
      OutputHandleFactory sinkFactory, Handle handle, ConnectorArguments arguments) {
    return new TaskRunContext(
        sinkFactory,
        handle,
        10,
        new TaskRunContextOps() {
          @Nonnull
          @Override
          public TaskState getTaskState(@Nonnull Task<?> task) {
            throw new UnsupportedOperationException("Not supported.");
          }

          @Override
          public <T> T runChildTask(@Nonnull Task<T> task) throws MetadataDumperUsageException {
            throw new UnsupportedOperationException("Not supported.");
          }
        },
        arguments);
  }

  public static TaskRunContext create(Handle handle) {
    try {
      return create(new DummyByteSinkFactory(), handle, new ConnectorArguments());
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }
}
