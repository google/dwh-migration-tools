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

import com.google.edwmigration.dumper.application.dumper.metrics.*;
import java.nio.file.FileSystem;
import java.util.UUID;

/**
 * TelemetryProcessor that uses the Strategy pattern to handle telemetry operations. This replaces
 * the boolean shouldWrite flag with a more flexible approach.
 */
public class TelemetryProcessor {
  private final String runId = UUID.randomUUID().toString();
  private final TelemetryWriteStrategy telemetryStrategy;

  /**
   * Constructor that takes a TelemetryStrategy instead of a boolean flag.
   *
   * @param telemetryStrategy the strategy to use for telemetry operations
   */
  public TelemetryProcessor(TelemetryWriteStrategy telemetryStrategy) {
    this.telemetryStrategy = telemetryStrategy;
  }

  public void setZipFilePathForDiskWriteStrategy(FileSystem fileSystem) {
    if (telemetryStrategy instanceof DiskTelemetryWriteStrategy) {
      ((DiskTelemetryWriteStrategy) telemetryStrategy).setZipFilePath(fileSystem);
    }
    telemetryStrategy.flush();
  }

  public void process(ClientTelemetry clientTelemetry) {
    telemetryStrategy.process(giveIdToClientTelemetry(clientTelemetry));
  }

  public void flush() {
    telemetryStrategy.flush();
  }

  private ClientTelemetry giveIdToClientTelemetry(ClientTelemetry clientTelemetry) {
    return ClientTelemetry.builder(clientTelemetry).setId(runId).build();
  }
}
