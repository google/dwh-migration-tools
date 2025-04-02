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
package com.google.edwmigration.validation.application.validator.task;

import static java.lang.String.format;

import com.google.common.base.Preconditions;
import com.google.edwmigration.validation.application.validator.ValidationArguments;
import com.google.edwmigration.validation.application.validator.handle.Handle;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public abstract class AbstractTargetTask {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSourceTask.class);

  private final Handle handle;
  private final URI outputUri;
  private final ValidationArguments arguments;

  public AbstractTargetTask(Handle handle, URI outputUri, ValidationArguments arguments) {
    Preconditions.checkNotNull(handle, "Handle is null.");
    this.handle = handle;
    this.outputUri = outputUri;
    this.arguments = arguments;
  }

  public Handle getHandle() {
    return handle;
  }

  public ValidationArguments getArguments() {
    return arguments;
  }

  public URI getOutputUri() {
    return outputUri;
  }

  public abstract void run() throws Exception;

  public String describeTargetData() {
    return "from" + getClass().getSimpleName();
  }

  public String toString() {
    return format("Write to %s %s", outputUri, describeTargetData());
  }
}
