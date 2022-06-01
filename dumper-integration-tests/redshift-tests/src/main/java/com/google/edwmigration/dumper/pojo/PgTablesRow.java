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

import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getBooleanNotNull;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getStringNotNull;
import static java.lang.System.lineSeparator;

import com.google.auto.value.AutoValue;
import com.google.edwmigration.dumper.csv.CsvUtil;
import java.sql.ResultSet;
import java.sql.SQLException;

/** POJO class for serialization data from DB and CSV files. */
@AutoValue
public abstract class PgTablesRow {

  public static PgTablesRow create(
      String schemaname,
      String tablename,
      String tableowner,
      String tablespace,
      boolean hasindexes,
      boolean hasrules,
      boolean hastriggers) {
    return new AutoValue_PgTablesRow(
        schemaname, tablename, tableowner, tablespace, hasindexes, hasrules, hastriggers);
  }

  public static PgTablesRow create(ResultSet rs) throws SQLException {
    return PgTablesRow.create(
        getStringNotNull(rs, "schemaname"),
        getStringNotNull(rs, "tablename"),
        getStringNotNull(rs, "tableowner"),
        getStringNotNull(rs, "tablespace"),
        getBooleanNotNull(rs, "hasindexes"),
        getBooleanNotNull(rs, "hasrules"),
        getBooleanNotNull(rs, "hastriggers"));
  }

  public static PgTablesRow create(String[] csvLine) {
    return new AutoValue_PgTablesRow(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getStringNotNull(csvLine[1]),
        CsvUtil.getStringNotNull(csvLine[2]),
        CsvUtil.getStringNotNull(csvLine[3]),
        CsvUtil.getBooleanNotNull(csvLine[4]),
        CsvUtil.getBooleanNotNull(csvLine[5]),
        CsvUtil.getBooleanNotNull(csvLine[6]));
  }

  public abstract String schemaname();

  public abstract String tablename();

  public abstract String tableowner();

  public abstract String tablespace();

  public abstract boolean hasindexes();

  public abstract boolean hasrules();

  public abstract boolean hastriggers();

  @Override
  public String toString() {
    return "schemaname="
        + schemaname()
        + ", tablename="
        + tablename()
        + ", tableowner="
        + tableowner()
        + ", tablespace="
        + tablespace()
        + ", hasindexes="
        + hasindexes()
        + ", hasrules="
        + hasrules()
        + ", hastriggers="
        + hastriggers()
        + lineSeparator();
  }
}
