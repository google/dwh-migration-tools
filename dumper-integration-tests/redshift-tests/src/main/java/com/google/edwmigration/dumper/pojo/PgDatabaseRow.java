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
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getIntNotNull;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getStringNotNull;
import static java.lang.System.lineSeparator;

import com.google.auto.value.AutoValue;
import com.google.edwmigration.dumper.csv.CsvUtil;
import java.sql.ResultSet;
import java.sql.SQLException;

/** POJO class for serialization data from DB and CSV files. */
@AutoValue
public abstract class PgDatabaseRow {

  public static PgDatabaseRow create(
      String datname,
      int datdba,
      int encoding,
      boolean datistemplate,
      boolean datallowconn,
      int datlastsysoid,
      int datvacuumxid,
      int datfrozenxid,
      int dattablespace,
      String datconfig,
      String datacl) {
    return new AutoValue_PgDatabaseRow(
        datname,
        datdba,
        encoding,
        datistemplate,
        datallowconn,
        datlastsysoid,
        datvacuumxid,
        datfrozenxid,
        dattablespace,
        datconfig,
        datacl);
  }

  public static PgDatabaseRow create(ResultSet rs) throws SQLException {
    return PgDatabaseRow.create(
        getStringNotNull(rs, "datname"),
        getIntNotNull(rs, "datdba"),
        getIntNotNull(rs, "encoding"),
        getBooleanNotNull(rs, "datistemplate"),
        getBooleanNotNull(rs, "datallowconn"),
        getIntNotNull(rs, "datlastsysoid"),
        getIntNotNull(rs, "datvacuumxid"),
        getIntNotNull(rs, "datfrozenxid"),
        getIntNotNull(rs, "dattablespace"),
        getStringNotNull(rs, "datconfig"),
        getStringNotNull(rs, "datacl"));
  }

  public static PgDatabaseRow create(String[] csvLine) {
    return new AutoValue_PgDatabaseRow(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getIntNotNull(csvLine[1]),
        CsvUtil.getIntNotNull(csvLine[2]),
        CsvUtil.getBooleanNotNull(csvLine[3]),
        CsvUtil.getBooleanNotNull(csvLine[4]),
        CsvUtil.getIntNotNull(csvLine[5]),
        CsvUtil.getIntNotNull(csvLine[6]),
        CsvUtil.getIntNotNull(csvLine[7]),
        CsvUtil.getIntNotNull(csvLine[8]),
        CsvUtil.getStringNotNull(csvLine[9]),
        CsvUtil.getStringNotNull(csvLine[10]));
  }

  public abstract String datname();

  public abstract int datdba();

  public abstract int encoding();

  public abstract boolean datistemplate();

  public abstract boolean datallowconn();

  public abstract int datlastsysoid();

  public abstract int datvacuumxid();

  public abstract int datfrozenxid();

  public abstract int dattablespace();

  public abstract String datconfig();

  public abstract String datacl();

  @Override
  public String toString() {
    return "datname="
        + datname()
        + ", datdba="
        + datdba()
        + ", encoding="
        + encoding()
        + ", datistemplate="
        + datistemplate()
        + ", datallowconn="
        + datallowconn()
        + ", datlastsysoid="
        + datlastsysoid()
        + ", datvacuumxid="
        + datvacuumxid()
        + ", datfrozenxid="
        + datfrozenxid()
        + ", dattablespace="
        + dattablespace()
        + ", datconfig="
        + datconfig()
        + ", datacl="
        + datacl()
        + lineSeparator();
  }
}
