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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.FORMAT_NAME;

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
import java.sql.ResultSet;
import java.time.Clock;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import org.apache.commons.csv.CSVFormat;

@AutoService(Connector.class)
@ParametersAreNonnullByDefault
public final class SnowflakeLiteConnector extends AbstractSnowflakeConnector {

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
    builder.add(proceduresTask());
    builder.add(warehouseEventsTask());
    builder.add(warehouseMeteringTask());

    for (AssessmentQuery item : planner.generateAssessmentQueries()) {
      String usageSchema = "SNOWFLAKE.ACCOUNT_USAGE";
      String query = String.format(item.formatString, usageSchema, /* an empty WHERE clause */ "");
      String zipName = item.zipEntryName;
      Task<?> task = new JdbcSelectTask(zipName, query).withHeaderTransformer(item.transformer());
      builder.add(task);
    }
    return builder.build();
  }

  private static Task<?> proceduresTask() {
    String view = "SNOWFLAKE.ACCOUNT_USAGE.PROCEDURES";
    String query =
        String.format(
            "SELECT procedure_language, procedure_owner, count(1) FROM %s GROUP BY ALL", view);
    ImmutableList<String> header = ImmutableList.of("Language", "Owner", "Count");
    return new LiteTimeSeriesTask("procedures.csv", query, header);
  }

  private static Task<?> warehouseEventsTask() {
    String query =
        "SELECT event_name, cluster_number, warehouse_id, warehouse_name, count(1)"
            + " FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY GROUP BY ALL";
    ImmutableList<String> header =
        ImmutableList.of("Name", "Cluster", "WarehouseId", "WarehouseName", "Count");
    return new LiteTimeSeriesTask("warehouse_events.csv", query, header);
  }

  private static Task<?> warehouseMeteringTask() {
    String view = "SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY";
    String query = String.format("SELECT warehouse_name, count(1) FROM %s GROUP BY ALL", view);
    ImmutableList<String> header = ImmutableList.of("Name", "Count");
    return new LiteTimeSeriesTask("warehouse_metering.csv", query, header);
  }

  private static final class LiteTimeSeriesTask extends JdbcSelectTask {

    private final ImmutableList<String> header;

    LiteTimeSeriesTask(String csvName, String sql, ImmutableList<String> header) {
      super(csvName, sql);
      this.header = header;
    }

    @Override
    @Nonnull
    protected CSVFormat newCsvFormat(ResultSet rs) {
      return FORMAT.builder().setHeader(header.toArray(new String[0])).build();
    }
  }
}
