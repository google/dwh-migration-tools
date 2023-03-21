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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/** @author shevek */
@Immutable // Not technically, because value might be mutable.
public class TaskResult<T> {

  private final TaskState state;
  private final T value;
  private final Throwable exception;

  public TaskResult(@Nonnull TaskState state, @CheckForNull T value) {
    this.state = Preconditions.checkNotNull(state);
    this.value = value;
    this.exception = null;
  }

  public TaskResult(@Nonnull TaskState state, @CheckForNull Exception exception) {
    this.state = Preconditions.checkNotNull(state);
    this.value = null;
    this.exception = exception;
  }

  @Nonnull
  public TaskState getState() {
    return state;
  }

  @CheckForNull
  public T getValue() {
    return value;
  }

  @CheckForNull
  public Throwable getException() {
    return exception;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("state", getState())
        .add("value", getValue())
        .add("exception", getException())
        .toString();
  }
}
