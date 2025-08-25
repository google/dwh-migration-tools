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
package com.google.edwmigration.dumper.application.dumper;

import com.google.common.base.Stopwatch;
import com.google.edwmigration.dumper.application.dumper.metrics.*;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import java.nio.file.FileSystem;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelemetryProcessor {
  private static final Logger logger = LoggerFactory.getLogger(TelemetryProcessor.class);

  private final ClientTelemetry clientTelemetry;
  private final boolean shouldWrite;

  public TelemetryProcessor(boolean shouldWrite) {
    clientTelemetry = new ClientTelemetry();
    clientTelemetry.setDumperMetadata(StartUpMetaInfoProcessor.getDumperMetadata());

    this.shouldWrite = shouldWrite;
  }

  /**
   * Generates a list of strings representing the summary for the current run. This summary does NOT
   * include the final ZIP file size, as it's generated before the ZIP is closed.
   */
  public void addDumperRunMetricsToPayload(
      ConnectorArguments arguments, TaskSetState state, Stopwatch stopwatch, boolean success) {
    if (!shouldWrite) {
      return;
    }
    try {
      clientTelemetry.setEventType(EventType.DUMPER_RUN_METRICS);

      List<TaskExecutionSummary> taskExecutionSummaries =
          state.getTasksReports().stream()
              .map(
                  tasksReport ->
                      new TaskExecutionSummary(tasksReport.count(), tasksReport.state().name()))
              .collect(Collectors.toList());

      List<TaskDetailedSummary> taskDetailedSummaries =
          state.getTaskResultSummaries().stream()
              .map(
                  item ->
                      new TaskDetailedSummary(
                          item.getTask().getName(),
                          item.getTask().getCategory().name(),
                          item.getTaskState().name(),
                          item.getThrowable().isPresent()
                              ? item.getThrowable().get().getMessage()
                              : null))
              .collect(Collectors.toList());

      Duration elapsed = stopwatch.elapsed();

      DumperRunMetrics dumperRunMetrics =
          DumperRunMetrics.builder()
              .setId(UUID.randomUUID().toString())
              .setMeasureStartTime(ZonedDateTime.now().minus(elapsed))
              .setRunDurationInMinutes(elapsed.getSeconds() / 60)
              .setOverallStatus(success ? "SUCCESS" : "FAILURE")
              .setTaskExecutionSummary(taskExecutionSummaries)
              .setTaskDetailedSummary(taskDetailedSummaries)
              .setArguments(arguments)
              .build();
      clientTelemetry.addToPayload(dumperRunMetrics);
    } catch (Exception e) {
      logger.warn("Failed to generate dumperRunMetrics and add it to payload", e);
    }
  }

  public void processTelemetry(FileSystem fileSystem) {
    if (!shouldWrite) {
      return;
    }
    try {
      TelemetryWriter.write(fileSystem, clientTelemetry);
    } catch (Exception e) {
      logger.warn("Failed to write telemetry", e);
    }
  }
}
