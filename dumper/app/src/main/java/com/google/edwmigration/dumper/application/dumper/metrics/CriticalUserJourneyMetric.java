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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class CriticalUserJourneyMetric {

  @JsonProperty private String id;

  @JsonProperty private LocalDateTime runStartTime;

  @JsonProperty private Long runDurationInSeconds;

  @JsonProperty private String overallStatus;

  @JsonProperty private List<TaskExecutionSummary> taskExecutionSummary;

  @JsonProperty private List<TaskDetailedSummary> taskDetailedSummary;

  @JsonProperty private ArgumentSummary argumentSummary;

  // Private constructor to be used by the builder
  private CriticalUserJourneyMetric(Builder builder) {
    this.id = builder.id;
    this.runStartTime = builder.runStartTime;
    this.runDurationInSeconds = builder.runDurationInSeconds;
    this.overallStatus = builder.overallStatus;
    this.taskExecutionSummary = builder.taskExecutionSummary;
    this.taskDetailedSummary = builder.taskDetailedSummary;
    this.argumentSummary = builder.argumentSummary;
  }

  // Getters
  public String getId() {
    return id;
  }

  public LocalDateTime getRunStartTime() {
    return runStartTime;
  }

  public Long getRunDurationInSeconds() {
    return runDurationInSeconds;
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

  public ArgumentSummary getArgumentProperties() {
    return argumentSummary;
  }

  // Static method to get a new builder instance
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String id;
    private LocalDateTime runStartTime;
    private Long runDurationInSeconds;
    private String overallStatus;
    private List<TaskExecutionSummary> taskExecutionSummary = new ArrayList<>();
    private List<TaskDetailedSummary> taskDetailedSummary = new ArrayList<>();
    private ArgumentSummary argumentSummary;

    public Builder setId(String id) {
      this.id = id;
      return this;
    }

    public Builder setRunStartTime(LocalDateTime runStartTime) {
      this.runStartTime = runStartTime;
      return this;
    }

    public Builder setRunDurationInSeconds(Long runDurationInSeconds) {
      this.runDurationInSeconds = runDurationInSeconds;
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

    public Builder addTaskExecutionSummary(TaskExecutionSummary summary) {
      if (this.taskExecutionSummary == null) {
        this.taskExecutionSummary = new ArrayList<>();
      }
      this.taskExecutionSummary.add(summary);
      return this;
    }

    public Builder setTaskDetailedSummary(List<TaskDetailedSummary> taskDetailedSummary) {
      this.taskDetailedSummary = taskDetailedSummary;
      return this;
    }

    public Builder addTaskDetailedSummary(TaskDetailedSummary summary) {
      if (this.taskDetailedSummary == null) {
        this.taskDetailedSummary = new ArrayList<>();
      }
      this.taskDetailedSummary.add(summary);
      return this;
    }

    public Builder setArguments(ConnectorArguments arguments) {
      if (arguments == null) {
        return this;
      }
      this.argumentSummary = new ArgumentSummary(arguments);
      return this;
    }

    public CriticalUserJourneyMetric build() {
      // You can add validation logic here before creating the object
      // For example, check for nulls for required fields
      return new CriticalUserJourneyMetric(this);
    }
  }
}
