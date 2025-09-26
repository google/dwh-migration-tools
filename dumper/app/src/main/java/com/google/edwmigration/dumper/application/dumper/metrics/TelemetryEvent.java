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
package com.google.edwmigration.dumper.application.dumper.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

public class TelemetryEvent {

  @JsonProperty private final String runId;

  @JsonProperty private final String eventId = UUID.randomUUID().toString();

  @JsonProperty private final EventType eventType;

  @JsonProperty private final Long timestamp;

  @JsonProperty private final TelemetryPayload payload;

  private TelemetryEvent(Builder builder) {
    this.runId = builder.runId;
    this.eventType = builder.eventType;
    this.timestamp = builder.timestamp;
    this.payload = builder.payload;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(TelemetryEvent clientTelemetry) {
    return new Builder()
        .setEventType(clientTelemetry.getEventType())
        .setTimestamp(clientTelemetry.getTimestamp())
        .setPayload(clientTelemetry.getPayload());
  }

  public EventType getEventType() {
    return eventType;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public TelemetryPayload getPayload() {
    return payload;
  }

  public static class Builder {
    private String runId;
    private EventType eventType = EventType.DUMPER_INIT;
    private Long timestamp = System.currentTimeMillis();
    private TelemetryPayload payload;

    public Builder setRunId(String runId) {
      this.runId = runId;
      return this;
    }

    public Builder setEventType(EventType eventType) {
      this.eventType = eventType;
      return this;
    }

    public Builder setTimestamp(Long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder setPayload(TelemetryPayload payload) {
      this.payload = payload;
      return this;
    }

    public TelemetryEvent build() {
      return new TelemetryEvent(this);
    }
  }
}
