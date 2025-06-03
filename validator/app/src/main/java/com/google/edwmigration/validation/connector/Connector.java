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
package com.google.edwmigration.validation.connector;

import com.google.edwmigration.validation.NameManager;
import com.google.edwmigration.validation.ValidationArguments;
import com.google.edwmigration.validation.ValidationConnection;
import com.google.edwmigration.validation.handle.Handle;
import com.google.edwmigration.validation.task.AbstractSourceTask;
import com.google.edwmigration.validation.task.AbstractTargetTask;
import java.net.URI;
import javax.annotation.Nonnull;

public interface Connector {

  @Nonnull
  String getName();

  @Nonnull
  Handle open(@Nonnull ValidationConnection arguments) throws Exception;

  @Nonnull
  AbstractSourceTask getSourceQueryTask(
      Handle handle, URI outputUri, ValidationArguments arguments);

  @Nonnull
  AbstractTargetTask getTargetQueryTask(
      Handle handle, NameManager nameManager, ValidationArguments arguments);
}
