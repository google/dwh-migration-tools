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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakePlanner.AssessmentQuery;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil;
import java.time.Clock;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@AutoService(Connector.class)
@ParametersAreNonnullByDefault
public final class SnowflakeLiteConnector extends AbstractSnowflakeConnector {

  private static final String FORMAT_NAME = "snowflake-lite.zip";
  private static final String NAME = "snowflake-lite";

  private final SnowflakePlanner planner = new SnowflakePlanner();

  public SnowflakeLiteConnector() {
    super(NAME);
  }

  @Override
  @Nonnull
  public String getDefaultFileName(boolean isAssessment, @Nullable Clock clock) {
    return ArchiveNameUtil.getFileName(NAME);
  }

  @Override
  public final void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    out.addAll(createTaskList());
  }

  private ImmutableList<Task<?>> createTaskList() {
    ImmutableList.Builder<Task<?>> builder = ImmutableList.builder();

    builder.addAll(planner.generateLiteSpecificQueries());
    builder.add(planner.proceduresTask());
    builder.add(planner.reportDateRangeTask());
    builder.add(planner.eventStateTask());
    builder.add(planner.operationEndsTask());
    builder.add(planner.operationStartsTask());
    builder.add(planner.warehouseEventsHistoryTask());
    builder.add(planner.warehouseEventsTask());
    builder.add(planner.warehouseMeteringTask());

    for (AssessmentQuery item : planner.generateAssessmentQueries()) {
      String usageSchema = "SNOWFLAKE.ACCOUNT_USAGE";
      String query = String.format(item.formatString, usageSchema, /* an empty WHERE clause */ "");
      String zipName = item.zipEntryName;
      Task<?> task = new JdbcSelectTask(zipName, query).withHeaderTransformer(item.transformer());
      builder.add(task);
    }
    return builder.build();
  }
}
