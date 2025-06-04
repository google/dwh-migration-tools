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
package com.google.edwmigration.validation.core;

import com.google.edwmigration.validation.config.BqTargetTable;
import com.google.edwmigration.validation.config.GoogleCloudStorage;
import com.google.edwmigration.validation.config.ResultTable;
import com.google.edwmigration.validation.config.SourceConnection;
import com.google.edwmigration.validation.config.SourceTable;
import com.google.edwmigration.validation.config.ValidationConfig;
import com.google.edwmigration.validation.deformed.*;
import com.google.edwmigration.validation.logging.Logger;
import com.google.edwmigration.validation.model.NamedTask;
import com.google.edwmigration.validation.model.ValidationStep;
import java.util.List;

public class Orchestrator {
  private final ValidationConfig config;

  public Orchestrator(ValidationConfig config) {
    this.config = config;
  }

  public List<ValidationStep> buildTasks() {
    return List.of(
        ValidationStep.of("Verifying Configuration", buildVerifyTasks()),
        ValidationStep.of("Connecting to source DB", List.of()),
        ValidationStep.of("Connecting to target DB", List.of()),
        ValidationStep.of("Comparing Schemas", List.of()),
        ValidationStep.of("Comparing Tables", List.of()),
        ValidationStep.of("Comparing Row Counts", List.of()),
        ValidationStep.of("Running Row-Level Comparisons", List.of()),
        ValidationStep.of("Validation Complete", List.of()));
  }

  public NamedTask dingo() {
    return NamedTask.of(
        "Applying connection settings",
        () -> {
          try {

            // Print config to confirm it's loaded
            return true;
          } catch (Exception e) {
            Logger.error("Something went wrong", e);
            return false;
          }
        });
  }

  public List<NamedTask> buildVerifyTasks() {
    return List.of(
        NamedTask.of(
            "Verify Source Connection Settings",
            () -> {
              Deformed<SourceConnection> v = new Deformed<>(SourceConnection.buildSchema());
              v.validate(config.SourceConnection);
              if (!v.isValid) {
                Logger.error(ErrorFormatter.format(v.validationState));
                return false;
              }
              return true;
            }),
        NamedTask.of(
            "Verify Source Table Settings",
            () -> {
              Deformed<SourceTable> v = new Deformed<>(SourceTable.buildSchema());
              v.validate(config.SourceTable);
              if (!v.isValid) {
                Logger.error(ErrorFormatter.format(v.validationState));
                return false;
              }
              return true;
            }),
        NamedTask.of(
            "Verify BQ Target Table Settings",
            () -> {
              Deformed<BqTargetTable> v = new Deformed<>(BqTargetTable.buildSchema());
              v.validate(config.BqTargetTable);
              if (!v.isValid) {
                Logger.error(ErrorFormatter.format(v.validationState));
                return false;
              }
              return true;
            }),
        NamedTask.of(
            "Verify GCS Path Settings",
            () -> {
              Deformed<GoogleCloudStorage> v = new Deformed<>(GoogleCloudStorage.buildSchema());
              v.validate(config.GoogleCloudStorage);
              if (!v.isValid) {
                Logger.error(ErrorFormatter.format(v.validationState));
                return false;
              }
              return true;
            }),
        NamedTask.of(
            "Verify Result Table Settings",
            () -> {
              Deformed<ResultTable> v = new Deformed<>(ResultTable.buildSchema());
              v.validate(config.ResultTable);
              if (!v.isValid) {
                Logger.error(ErrorFormatter.format(v.validationState));
                return false;
              }
              Logger.debug("Source Connection:");
              Logger.debug("    ├── Source DB: " + config.SourceConnection.connectionType);
              Logger.debug("    ├── Host: " + config.SourceConnection.host);
              Logger.debug("    ├── Database: " + config.SourceConnection.database);
              Logger.debug("    ├── User: " + config.SourceConnection.user);
              Logger.debug("    └── Port: " + config.SourceConnection.port);
              Logger.debug("Google Cloud Storage:");
              Logger.debug("    └── GCS Path: " + config.GoogleCloudStorage.gcsPath);
              Logger.debug("Source Table:");
              Logger.debug("    ├── Source Schema: " + config.SourceTable.schema);
              Logger.debug("    ├── Source Name: " + config.SourceTable.name);
              Logger.debug("    └── Primary Key: " + config.SourceTable.primaryKey);
              Logger.debug("BigQuery Target Table:");
              Logger.debug("    ├── Project Id: " + config.BqTargetTable.projectId);
              Logger.debug("    ├── Dataset: " + config.BqTargetTable.dataset);
              Logger.debug("    ├── Schema: " + config.BqTargetTable.schema);
              Logger.debug("    ├── Name: " + config.BqTargetTable.name);
              Logger.debug("    └── Primary Key: " + config.SourceTable.primaryKey);
              Logger.debug("Result Table:");
              Logger.debug("    └── Results Table Location: " + config.ResultTable.name);
              return true;
            }));
  }
}
