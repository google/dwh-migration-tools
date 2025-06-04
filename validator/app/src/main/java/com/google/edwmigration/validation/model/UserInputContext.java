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
package com.google.edwmigration.validation.model;

import com.google.edwmigration.validation.config.BqTargetTable;
import com.google.edwmigration.validation.config.ColumnMapping;
import com.google.edwmigration.validation.config.GoogleCloudStorage;
import com.google.edwmigration.validation.config.ResultTable;
import com.google.edwmigration.validation.config.SourceConnection;
import com.google.edwmigration.validation.config.SourceTable;
import com.google.edwmigration.validation.deformed.Deformed;
import com.google.edwmigration.validation.deformed.ValidationProperty;
import com.google.edwmigration.validation.deformed.ValidationSchema;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class UserInputContext {
  public final SourceConnection sourceConnection;
  public final GoogleCloudStorage googleCloudStorage;
  public final BqTargetTable bqTargetTable;
  public final SourceTable sourceTable;
  public final ResultTable resultTable;
  public final ColumnMapping columnMapping;

  private UserInputContext(
      SourceConnection sourceConnection,
      GoogleCloudStorage googleCloudStorage,
      BqTargetTable bqTargetTable,
      SourceTable sourceTable,
      ResultTable resultTable,
      ColumnMapping columnMapping) {
    this.sourceConnection = sourceConnection;
    this.googleCloudStorage = googleCloudStorage;
    this.bqTargetTable = bqTargetTable;
    this.sourceTable = sourceTable;
    this.resultTable = resultTable;
    this.columnMapping = columnMapping;
  }

  public static UserInputContext of(
      SourceConnection sourceConnection,
      GoogleCloudStorage googleCloudStorage,
      BqTargetTable bqTargetTable,
      SourceTable sourceTable,
      ResultTable resultTable,
      ColumnMapping columnMapping) {
    return new UserInputContext(
        sourceConnection,
        googleCloudStorage,
        bqTargetTable,
        sourceTable,
        resultTable,
        columnMapping);
  }

  private static <S, T> ValidationProperty<S> errors(
      String fieldName, Deformed<T> nestedValidator, Function<S, T> selector) {

    return ValidationProperty.of(
        s -> {
          T value = selector.apply(s);
          if (value == null) return fieldName + " fields are required.";
          nestedValidator.validate(value);

          return nestedValidator.validationState.getAll().entrySet().stream()
              .filter(e -> !e.getValue().isValid)
              .flatMap(e -> e.getValue().errors.stream())
              .collect(Collectors.joining(", "));
        },
        s -> {
          T value = selector.apply(s);
          return value != null && nestedValidator.validate(value);
        });
  }

  // TODO validate ColumnMapping
  public static ValidationSchema<UserInputContext> buildSchema() {
    Deformed<SourceConnection> sc = new Deformed<>(SourceConnection.buildSchema());
    Deformed<SourceTable> st = new Deformed<>(SourceTable.buildSchema());
    Deformed<BqTargetTable> bqt = new Deformed<>(BqTargetTable.buildSchema());
    Deformed<GoogleCloudStorage> gcs = new Deformed<>(GoogleCloudStorage.buildSchema());
    Deformed<ResultTable> rt = new Deformed<>(ResultTable.buildSchema());

    return new ValidationSchema<>(
        Map.of(
            "sourceConnection", List.of(errors("sourceConnection", sc, s -> s.sourceConnection)),
            "sourceTable", List.of(errors("sourceTable", st, s -> s.sourceTable)),
            "bqTargetTable", List.of(errors("bqTargetTable", bqt, s -> s.bqTargetTable)),
            "resultTable", List.of(errors("resultTable", rt, s -> s.resultTable)),
            "googleCloudStorage",
                List.of(errors("googleCloudStorage", gcs, s -> s.googleCloudStorage))));
  }
}
