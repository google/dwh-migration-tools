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
package com.google.edwmigration.dumper.application.dumper.connector.oracle.task;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.edwmigration.dumper.application.dumper.task.TaskCategory.OPTIONAL;
import static com.google.edwmigration.dumper.application.dumper.task.TaskState.SUCCEEDED;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.oracle.QueryGroup;
import com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import com.google.edwmigration.dumper.application.dumper.task.TaskState;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class ResultMessageTask extends VoidTask {

  private final Condition condition;
  private final QueryGroup group;

  private ResultMessageTask(Condition condition, QueryGroup group) {
    this.condition = condition;
    this.group = group;
  }

  public static VoidTask create(QueryGroup group, List<StatsJdbcTask> tasks) {
    List<StatsJdbcTask> matches = StatsJdbcTask.findByGroup(tasks, group);
    Condition condition = onAllTasks(matches, SUCCEEDED);
    return new ResultMessageTask(condition, group);
  }

  @Override
  @Nonnull
  public Condition[] getConditions() {
    return new Condition[] {condition};
  }

  @Override
  @Nonnull
  public String getTargetPath() {
    return String.format("loading of group %s", group);
  }

  @Nonnull
  @Override
  public String toString() {
    String tenantSetup = group.tenantSetup().code;
    return String.format("Load %s data (%s version).", group.statsSource(), tenantSetup);
  }

  @Override
  @Nonnull
  public TaskCategory getCategory() {
    return OPTIONAL;
  }

  @Override
  void doRun(TaskRunContext context) {}

  private static Condition onAllTasks(List<StatsJdbcTask> tasks, TaskState requiredState) {
    if (tasks.isEmpty()) {
      return emptyGroupFailedCondition();
    }
    ImmutableList<Condition> conditions =
        tasks.stream()
            .map(item -> new StateCondition(item, requiredState))
            .collect(toImmutableList());
    return new AndCondition(conditions);
  }

  private static Condition emptyGroupFailedCondition() {
    return new Condition() {

      @Override
      public boolean evaluate(@Nonnull TaskSetState state) {
        return false;
      }

      @Override
      public String toString() {
        return "group is non-empty";
      }
    };
  }
}
