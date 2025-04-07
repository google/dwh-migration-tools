/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.validation.application.validator.sql;

import com.google.edwmigration.validation.application.validator.ValidationTableMapping.ValidationTable;
import java.util.HashMap;
import javax.annotation.Nullable;
import org.jooq.DSLContext;
import org.jooq.DataType;

/** @author nehanene */
public interface SqlGenerator {

  DSLContext getDSLContext();

  ValidationTable getValidationTable();

  String getPrimaryKey();

  String getAggregateQuery(HashMap<String, DataType<? extends Number>> numericColumns);

  String getRowSampleQuery();

  DataType<? extends Number> getSqlDataType(
      String dataType, @Nullable Integer numericPrecision, @Nullable Integer numericScale);
}
