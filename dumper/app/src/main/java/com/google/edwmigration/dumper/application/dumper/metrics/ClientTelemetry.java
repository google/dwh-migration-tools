/*
 * Copyright 2022-2025 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

  @JsonProperty private final String id;
  @JsonProperty private final String eventId;
  @JsonProperty private final EventType eventType;
  @JsonProperty private final ZonedDateTime timestamp;
  @JsonProperty private final List<TelemetryPayload> payload;

  private ClientTelemetry(Builder builder) {
    this.id = builder.id;
    this.eventId = builder.eventId;
    this.eventType = builder.eventType;
    this.timestamp = builder.timestamp;
    this.payload = builder.payload;
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

  public ZonedDateTime timestamp() {
    return timestamp;
  }

  public List<TelemetryPayload> getPayload() {
    return payload;
  }

  public static class Builder {
    private String id;
    private String eventId;
    private EventType eventType;
    private ZonedDateTime timestamp = ZonedDateTime.now();
    private List<TelemetryPayload> payload = new ArrayList<>();

    public Builder id(String id) {
      this.id = id;
      return this;
    }

    public Builder eventId(String eventId) {
      this.eventId = eventId;
      return this;
    }

    public Builder timestamp(ZonedDateTime timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder eventType(EventType eventType) {
      this.eventType = eventType;
      return this;
    }

    public Builder payload(List<TelemetryPayload> payload) {
      this.payload = payload;
      return this;
    }

    public ClientTelemetry build() {
        return new ClientTelemetry(this);
      }
    }
  }