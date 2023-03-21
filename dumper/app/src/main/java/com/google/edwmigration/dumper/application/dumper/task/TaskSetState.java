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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/** @author shevek */
public interface TaskSetState {

  @ThreadSafe
  public static class Impl implements TaskSetState {

    @GuardedBy("lock")
    private final Map<Task<?>, TaskResult<?>> resultMap = new HashMap<>();

    private final Object lock = new Object();

    @Nonnull
    public Map<Task<?>, TaskResult<?>> getTaskResultMap() {
      synchronized (lock) {
        return resultMap;
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> TaskResult<T> getTaskResult(@Nonnull Task<T> task) {
      synchronized (lock) {
        return (TaskResult<T>) resultMap.get(task);
      }
    }

    public <T> void setTaskResult(
        @Nonnull Task<T> task, @Nonnull TaskState state, @CheckForNull T value) {
      synchronized (lock) {
        resultMap.put(task, new TaskResult<>(state, value));
      }
    }

    public <T> void setTaskException(
        @Nonnull Task<T> task, @Nonnull TaskState state, @CheckForNull Exception exception) {
      synchronized (lock) {
        resultMap.put(task, new TaskResult<>(state, exception));
      }
    }
  }

  @CheckForNull
  public <T> TaskResult<T> getTaskResult(@Nonnull Task<T> task);

  @Nonnull
  public default TaskState getTaskState(@Nonnull Task<?> task) {
    TaskResult<?> result = getTaskResult(task);
    return (result == null) ? TaskState.NOT_STARTED : result.getState();
  }
}
