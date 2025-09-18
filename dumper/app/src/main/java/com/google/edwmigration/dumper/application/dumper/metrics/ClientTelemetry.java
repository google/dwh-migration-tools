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
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class ClientTelemetry {

  // Uniquer run Id. Is Set by the TelemetryProcessor.
  @JsonProperty private String id;

  @JsonProperty private String eventId;

  @JsonProperty private EventType eventType;

  @JsonProperty private ZonedDateTime timestamp;

  @JsonProperty private List<TelemetryPayload> payload;

  private ClientTelemetry(Builder builder) {
    this.id = builder.id;
    this.eventId = builder.eventId;
    this.eventType = builder.eventType;
    this.timestamp = builder.timestamp;
    this.payload = builder.payload;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(ClientTelemetry clientTelemetry) {
    return new Builder()
        .setEventId(clientTelemetry.getEventId())
        .setEventType(clientTelemetry.getEventType())
        .setTimestamp(clientTelemetry.getTimestamp())
        .setPayload(clientTelemetry.getPayload());
  }

  public String getId() {
    return id;
  }

  public String getEventId() {
    return eventId;
  }

  public EventType getEventType() {
    return eventType;
  }

  public ZonedDateTime getTimestamp() {
    return timestamp;
  }

  public List<TelemetryPayload> getPayload() {
    return payload;
  }

  public static class Builder {
    private String id;
    private String eventId;
    private EventType eventType = EventType.METADATA;
    private ZonedDateTime timestamp = ZonedDateTime.now();
    private List<TelemetryPayload> payload = new ArrayList<>();

    public Builder setId(String id) {
      this.id = id;
      return this;
    }

    public Builder setEventId(String eventId) {
      this.eventId = eventId;
      return this;
    }

    public Builder setEventType(EventType eventType) {
      this.eventType = eventType;
      return this;
    }

    public Builder setTimestamp(ZonedDateTime timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder setPayload(List<TelemetryPayload> payload) {
      this.payload = payload != null ? new ArrayList<>(payload) : new ArrayList<>();
      return this;
    }

    public ClientTelemetry build() {
      return new ClientTelemetry(this);
    }
  }
}
