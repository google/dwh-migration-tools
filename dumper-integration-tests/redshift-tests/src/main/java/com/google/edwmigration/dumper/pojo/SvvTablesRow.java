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

import static com.google.edwmigration.dumper.csv.CsvUtil.getStringNotNull;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getStringNotNull;
import static java.lang.System.lineSeparator;

import com.google.auto.value.AutoValue;
import java.sql.ResultSet;
import java.sql.SQLException;

/** POJO class for serialization data from DB and CSV files. */
@AutoValue
public abstract class SvvTablesRow {

  public static SvvTablesRow create(
      String tableCatalog,
      String tableSchema,
      String tableName,
      String tableType,
      String remarks) {
    return new AutoValue_SvvTablesRow(
        tableCatalog,
        tableSchema,
        tableName,
        tableType,
        remarks);
  }

  public static SvvTablesRow create(ResultSet rs) throws SQLException {
    return SvvTablesRow.create(
        getStringNotNull(rs, "table_catalog"),
        getStringNotNull(rs, "table_schema"),
        getStringNotNull(rs, "table_name"),
        getStringNotNull(rs, "table_type"),
        getStringNotNull(rs, "remarks"));
  }

  public static SvvTablesRow create(String[] csvLine) {
    return SvvTablesRow.create(
        getStringNotNull(csvLine[0]),
        getStringNotNull(csvLine[1]),
        getStringNotNull(csvLine[2]),
        getStringNotNull(csvLine[3]),
        getStringNotNull(csvLine[4]));
  }

  public abstract String tableCatalog();

  public abstract String tableSchema();

  public abstract String tableName();

  public abstract String tableType();

  public abstract String remarks();

  @Override
  public String toString() {
    return "tableCatalog="
        + tableCatalog()
        + ", tableSchema="
        + tableSchema()
        + ", tableName="
        + tableName()
        + ", tableType="
        + tableType()
        + ", remarks="
        + remarks()
        + lineSeparator();
  }
}
