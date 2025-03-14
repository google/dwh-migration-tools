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
package com.google.edwmigration.dumper.application.dumper.connector;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.time.Clock;
import java.util.List;
import javax.annotation.Nonnull;

/** @author shevek */
public interface Connector {

  // Empty
  enum DefaultProperties implements ConnectorProperty {}

  @Nonnull
  default String getDescription() {
    return "";
  }

  @Nonnull
  String getName();

  @Nonnull
  String getDefaultFileName(boolean isAssessment, Clock clock);

  /**
   * Validates if the cli parameters passed to the particular connector are expected. The method is
   * called before {@link Connector#open(ConnectorArguments)} and {@link Connector#addTasksTo(List,
   * ConnectorArguments)}
   *
   * @param arguments cli params
   * @throws IllegalStateException if incorrect set of arguments passed to the particular connector
   */
  default void validate(ConnectorArguments arguments) throws IllegalStateException {}

  void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception;

  @Nonnull
  Handle open(@Nonnull ConnectorArguments arguments) throws Exception;

  @Nonnull
  Iterable<ConnectorProperty> getPropertyConstants();
}
