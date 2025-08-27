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

/**
 * TelemetryProcessor that uses the Strategy pattern to handle telemetry operations. This replaces
 * the boolean shouldWrite flag with a more flexible approach.
 */
public class TelemetryProcessor {
  private final ClientTelemetry clientTelemetry;
  private final TelemetryStrategy telemetryStrategy;

  /**
   * Constructor that takes a TelemetryStrategy instead of a boolean flag.
   *
   * @param telemetryStrategy the strategy to use for telemetry operations
   */
  public TelemetryProcessor(TelemetryStrategy telemetryStrategy) {
    this.telemetryStrategy = telemetryStrategy;
    clientTelemetry = new ClientTelemetry();
    clientTelemetry.setDumperMetadata(StartUpMetaInfoProcessor.getDumperMetadata());
  }

  /**
   * Generates a list of strings representing the summary for the current run. This summary does NOT
   * include the final ZIP file size, as it's generated before the ZIP is closed.
   */
  public void addDumperRunMetricsToPayload(
      ConnectorArguments arguments, TaskSetState state, Stopwatch stopwatch, boolean success) {
    telemetryStrategy.processDumperRunMetrics(
        clientTelemetry, arguments, state, stopwatch, success);
  }

  public void addTaskTelemetry(TaskRunMetrics taskMetrics) {
    clientTelemetry.addToPayload(taskMetrics);
  }

  public void processTelemetry(FileSystem fileSystem) {
    telemetryStrategy.writeTelemetry(fileSystem, clientTelemetry);
  }
}
