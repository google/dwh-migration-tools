/*
 * Copyright 2022 Google LLC
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
package com.google.edwmigration.dumper.pojo;

import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getStringNotNull;
import static java.lang.System.lineSeparator;

import com.google.auto.value.AutoValue;
import com.google.edwmigration.dumper.csv.CsvUtil;
import java.sql.ResultSet;
import java.sql.SQLException;

/** POJO class for serialization data from DB and CSV files. */
@AutoValue
public abstract class PgProcRow {

  public static PgProcRow create(
      String schema,
      String name,
      String result_data_type,
      String argument_data_types,
      String description) {
    return new AutoValue_PgProcRow(schema, name, result_data_type, argument_data_types, description);
  }

  public static PgProcRow create(ResultSet rs) throws SQLException {
    return PgProcRow.create(
        getStringNotNull(rs, "schema"),
        getStringNotNull(rs, "name"),
        getStringNotNull(rs, "result_data_type"),
        getStringNotNull(rs, "argument_data_types"),
        getStringNotNull(rs, "description"));
  }

  public static PgProcRow create(String[] csvLine) {
    return PgProcRow.create(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getStringNotNull(csvLine[1]),
        CsvUtil.getStringNotNull(csvLine[2]),
        CsvUtil.getStringNotNull(csvLine[3]),
        CsvUtil.getStringNotNull(csvLine[4]));
  }

  public abstract String schema();

  public abstract String name();

  public abstract String resultDataType();

  public abstract String argumentDataTypes();

  public abstract String description();

  @Override
  public String toString() {
    return "schema="
        + schema()
        + ", name="
        + name()
        + ", resultDataType="
        + resultDataType()
        + ", argumentDataTypes="
        + argumentDataTypes()
        + ", description="
        + description()
        + lineSeparator();
  }

}
