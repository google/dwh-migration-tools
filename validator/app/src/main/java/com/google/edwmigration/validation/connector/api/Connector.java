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
package com.google.edwmigration.validation.connector.api;

import com.google.edwmigration.validation.connector.common.AbstractSourceTask;
import com.google.edwmigration.validation.connector.common.AbstractTargetTask;
import com.google.edwmigration.validation.io.writer.ResultSetWriterFactory;
import com.google.edwmigration.validation.model.ExecutionState;
import javax.annotation.Nonnull;

public interface Connector {

  @Nonnull
  String getName();

  @Nonnull
  Handle open(@Nonnull ExecutionState state) throws Exception;

  @Nonnull
  AbstractSourceTask getSourceQueryTask(ExecutionState state, ResultSetWriterFactory writerFactory);

  @Nonnull
  AbstractTargetTask getTargetQueryTask(ExecutionState state);
}
