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
import com.google.common.collect.Lists;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.checkerframework.common.value.qual.ArrayLen;

/** @author shevek */
public interface Task<T> {

  public static interface Condition {

    public static final Condition @ArrayLen(0) [] EMPTY_ARRAY = new Condition[0];

    public boolean evaluate(@Nonnull TaskSetState state);

    @Nonnull
    public default String toSkipReason() {
      return "[" + this + "] was not true";
    }
  }

  public static class StateCondition implements Condition {

    private final Task<?> task;
    private final TaskState taskState;

    public StateCondition(Task<?> task, TaskState taskState) {
      this.task = task;
      this.taskState = taskState;
      Preconditions.checkArgument(
          taskState != TaskState.NOT_STARTED, "Cannot accept NOT_STARTED as a precondition.");
    }

    @Override
    public boolean evaluate(TaskSetState state) {
      return state.getTaskState(task) == taskState;
    }

    @Override
    public String toSkipReason() {
      return "state of " + task.getName() + " was not " + taskState;
    }

    // This looks messed, but output of dry-run looks okay
    // this works for depth 1 , may work for more too !
    @Override
    public String toString() {
      return "\n    If "
          + taskState.toString()
          + (" Then\n  " + task.toString()).replaceAll("\n", "\n    ");
    }
  }

  public static class AndCondition implements Condition {

    private final List<Condition> conditions;

    public AndCondition(List<Condition> conditions) {
      this.conditions = Preconditions.checkNotNull(conditions, "Conditions was null.");
    }

    @Override
    public boolean evaluate(TaskSetState state) {
      for (Condition condition : conditions) if (!condition.evaluate(state)) return false;
      return true;
    }

    @Override
    public String toSkipReason() {
      return "all of " + Lists.transform(conditions, t -> t.toSkipReason());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("conditions", conditions).toString();
    }
  }

  @Nonnull
  public default String getName() {
    return getTargetPath();
  }

  @Nonnull
  public default TaskCategory getCategory() {
    return TaskCategory.REQUIRED;
  }

  @Nonnull
  public String getTargetPath();

  @Nonnull
  public default Condition[] getConditions() {
    return Condition.EMPTY_ARRAY;
  }

  @CheckForNull
  public T run(@Nonnull TaskRunContext context) throws Exception;

  // returns true if tasks handles the exception.
  // so it's not sent to user's screen
  public default boolean handleException(Exception Fe) {
    return false;
  }
}
