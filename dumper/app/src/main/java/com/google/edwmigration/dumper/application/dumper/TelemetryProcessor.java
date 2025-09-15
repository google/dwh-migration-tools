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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.edwmigration.dumper.application.dumper.metrics.ClientTelemetry.*;

/**
 * TelemetryProcessor that uses the Strategy pattern to handle telemetry operations. This replaces
 * the boolean shouldWrite flag with a more flexible approach.
 */
public class TelemetryProcessor {
  private final String runId = UUID.randomUUID().toString();
  private final TelemetryWriteStrategy telemetryWriteStrategy;

  /**
   * Constructor that takes a TelemetryStrategy instead of a boolean flag.
   *
   * @param telemetryWriteStrategy the strategy to use for telemetry operations
   */
  public TelemetryProcessor(TelemetryWriteStrategy telemetryWriteStrategy) {
    this.telemetryWriteStrategy = telemetryWriteStrategy;
    process(createStartEvent());
    process(createMetadataEvent());
  }

  public void process(ClientTelemetry clientTelemetry) {
    telemetryWriteStrategy.process(setIdToClientTelemetry(clientTelemetry));
  }

  /**
   * Generates a list of strings representing the summary for the current run. This summary does NOT
   * include the final ZIP file size, as it's generated before the ZIP is closed.
   */
  public void processTelemetry(FileSystem fileSystem) {
  }

  private ClientTelemetry setIdToClientTelemetry(ClientTelemetry clientTelemetry) {
    return new ClientTelemetry.Builder()
            .id(runId)
            .eventId(clientTelemetry.getEventId())
            .eventType(clientTelemetry.getEventType())
            .timestamp(clientTelemetry.timestamp())
            .payload(clientTelemetry.getPayload())
            .build();
  }

  private ClientTelemetry createMetadataEvent() {
    List<TelemetryPayload> arr = new ArrayList<>();
    arr.add(StartUpMetaInfoProcessor.getDumperMetadata());

    return new ClientTelemetry.Builder()
            .eventId(UUID.randomUUID().toString())
            .eventType(EventType.METADATA)
            .payload(arr)
            .build();
  }

  private ClientTelemetry createStartEvent() {
    return new ClientTelemetry.Builder()
            .eventId(UUID.randomUUID().toString())
            .eventType(EventType.DUMPER_START_EVENT)
            .build();
  }
}
