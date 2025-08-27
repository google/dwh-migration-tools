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
import java.time.Instant;

/**
 * keep immutable. TaskRunner is multi-threaded, so we need to make it thread-safe.
 *
 * @author kakha
 */
public class TaskRunMetrics implements TelemetryPayload {

  @JsonProperty private String name;

  @JsonProperty private EventType eventType = EventType.TASK_RUN_METRICS;

  @JsonProperty private String overallStatus;

  @JsonProperty private Instant measureStartInstance;

  @JsonProperty private Instant measureEndInstance;

  @JsonProperty private String error;

  public TaskRunMetrics() {
    // Default constructor for Jackson deserialization
  }

  public TaskRunMetrics(
      String name,
      String overallStatus,
      Instant measureStartInstance,
      Instant measureEndInstance,
      String error) {
    this.name = name;
    this.overallStatus = overallStatus;
    this.measureStartInstance = measureStartInstance;
    this.measureEndInstance = measureEndInstance;
    this.error = error;
  }

  public EventType getEventType() {
    return eventType;
  }

  public String getName() {
    return name;
  }

  public String getOverallStatus() {
    return overallStatus;
  }

  public Instant getMeasureStartInstance() {
    return measureStartInstance;
  }

  public Instant getMeasureEndInstance() {
    return measureEndInstance;
  }

  public String getError() {
    return error;
  }
}
