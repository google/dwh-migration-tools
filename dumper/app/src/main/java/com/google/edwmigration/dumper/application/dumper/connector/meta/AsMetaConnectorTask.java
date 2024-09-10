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
package com.google.edwmigration.dumper.application.dumper.connector.meta;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.nio.file.Paths;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

public class AsMetaConnectorTask<T> implements Task<T> {

  private final Task<T> underlyingTask;
  private final UnderlyingConnector underlyingConnector;
  private final String metaconnectorName;

  public AsMetaConnectorTask(
      @Nonnull Task<T> underlyingTask,
      UnderlyingConnector underlyingConnector,
      String metaconnectorName) {
    this.underlyingTask = underlyingTask;
    this.underlyingConnector = underlyingConnector;
    this.metaconnectorName = metaconnectorName;
  }

  @Nonnull
  @Override
  public String getName() {
    return underlyingTask.getName();
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    return underlyingTask.getCategory();
  }

  @Nonnull
  @Override
  public String getTargetPath() {
    return underlyingTask.getTargetPath();
  }

  @Nonnull
  @Override
  public Condition[] getConditions() {
    return underlyingTask.getConditions();
  }

  @CheckForNull
  @Override
  public T run(@Nonnull TaskRunContext context) throws Exception {
    MetaHandle metaHandle = (MetaHandle) context.getHandle();
    Handle connectorHandle = metaHandle.getHandleByConnectorName(underlyingConnector.name());
    ConnectorArguments childConnectorArguments =
        metaHandle.getArgumentsByConnectorName(underlyingConnector.name());
    return underlyingTask.run(
        context.forChildConnector(
            connectorHandle, Paths.get(underlyingConnector.path()), childConnectorArguments));
  }

  public boolean handleException(Exception e) {
    return underlyingTask.handleException(e);
  }

  @Override
  public String toString() {
    return metaconnectorName + ": " + underlyingTask;
  }
}
