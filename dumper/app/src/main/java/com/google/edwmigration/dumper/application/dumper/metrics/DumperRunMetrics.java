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
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class DumperRunMetrics implements TelemetryPayload {

  @JsonProperty private String id;

  @JsonProperty private ZonedDateTime measureStartTime;
  
  @JsonProperty private EventType eventType = EventType.DUMPER_RUN_METRICS;

  @JsonProperty private Long runDurationInMinutes;

  @JsonProperty private String overallStatus;

  @JsonProperty private List<TaskExecutionSummary> taskExecutionSummary;

  @JsonProperty private List<TaskDetailedSummary> taskDetailedSummary;

  @JsonProperty private ArgumentSummary argumentSummary;

  private DumperRunMetrics(Builder builder) {
    this.id = builder.id;
    this.measureStartTime = builder.measureStartTime;
    this.runDurationInMinutes = builder.runDurationInMinutes;
    this.overallStatus = builder.overallStatus;
    this.taskExecutionSummary = builder.taskExecutionSummary;
    this.taskDetailedSummary = builder.taskDetailedSummary;
    this.argumentSummary = builder.argumentSummary;
  }

  public String getId() {
    return id;
  }

  public ZonedDateTime getMeasureStartTime() {
    return measureStartTime;
  }

  public Long getRunDurationInMinutes() {
    return runDurationInMinutes;
  }

  public String getOverallStatus() {
    return overallStatus;
  }

  public List<TaskExecutionSummary> getTaskExecutionSummary() {
    return taskExecutionSummary;
  }

  public List<TaskDetailedSummary> getTaskDetailedSummary() {
    return taskDetailedSummary;
  }

  public ArgumentSummary getArgumentSummary() {
    return argumentSummary;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String id;
    private ZonedDateTime measureStartTime;
    private Long runDurationInMinutes;
    private String overallStatus;
    private List<TaskExecutionSummary> taskExecutionSummary = new ArrayList<>();
    private List<TaskDetailedSummary> taskDetailedSummary = new ArrayList<>();
    private ArgumentSummary argumentSummary;

    public Builder setId(String id) {
      this.id = id;
      return this;
    }

    public Builder setMeasureStartTime(ZonedDateTime measureStartTime) {
      this.measureStartTime = measureStartTime;
      return this;
    }

    public Builder setRunDurationInMinutes(Long runDurationInMinutes) {
      this.runDurationInMinutes = runDurationInMinutes;
      return this;
    }

    public Builder setOverallStatus(String overallStatus) {
      this.overallStatus = overallStatus;
      return this;
    }

    public Builder setTaskExecutionSummary(List<TaskExecutionSummary> taskExecutionSummary) {
      this.taskExecutionSummary = taskExecutionSummary;
      return this;
    }

    public Builder setTaskDetailedSummary(List<TaskDetailedSummary> taskDetailedSummary) {
      this.taskDetailedSummary = taskDetailedSummary;
      return this;
    }

    public Builder setArguments(ConnectorArguments arguments) {
      if (arguments == null) {
        return this;
      }
      this.argumentSummary = new ArgumentSummary(arguments);
      return this;
    }

    public DumperRunMetrics build() {
      return new DumperRunMetrics(this);
    }
  }
}
