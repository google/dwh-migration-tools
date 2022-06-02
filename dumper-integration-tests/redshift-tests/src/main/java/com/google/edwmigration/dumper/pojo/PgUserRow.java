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
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getTimestampNotNull;
import static java.lang.System.lineSeparator;

import com.google.auto.value.AutoValue;
import com.google.edwmigration.dumper.csv.CsvUtil;
import java.sql.ResultSet;
import java.sql.SQLException;

/** POJO class for serialization data from DB and CSV files. */
@AutoValue
public abstract class PgUserRow {

  public static PgUserRow create(
      String usename,
      int usesysid,
      boolean usecreatedb,
      boolean usesuper,
      boolean usecatupd,
      String passwd,
      long valuntil,
      String useconfig) {
    return new AutoValue_PgUserRow(
        usename, usesysid, usecreatedb, usesuper, usecatupd, passwd, valuntil, useconfig);
  }

  public static PgUserRow create(ResultSet rs) throws SQLException {
    return PgUserRow.create(
        getStringNotNull(rs, "usename"),
        getIntNotNull(rs, "usesysid"),
        getBooleanNotNull(rs, "usecreatedb"),
        getBooleanNotNull(rs, "usesuper"),
        getBooleanNotNull(rs, "usecatupd"),
        getStringNotNull(rs, "passwd"),
        getTimestampNotNull(rs, "valuntil"),
        getStringNotNull(rs, "useconfig"));
  }

  public static PgUserRow create(String[] csvLine) {
    return new AutoValue_PgUserRow(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getIntNotNull(csvLine[1]),
        CsvUtil.getBooleanNotNull(csvLine[2]),
        CsvUtil.getBooleanNotNull(csvLine[3]),
        CsvUtil.getBooleanNotNull(csvLine[4]),
        CsvUtil.getStringNotNull(csvLine[5]),
        CsvUtil.getTimestampNotNull(csvLine[6]),
        CsvUtil.getStringNotNull(csvLine[7]));
  }

  public abstract String usename();

  public abstract int usesysid();

  public abstract boolean usecreatedb();

  public abstract boolean usesuper();

  public abstract boolean usecatupd();

  public abstract String passwd();

  public abstract long valuntil();

  public abstract String useconfig();

  @Override
  public String toString() {
    return "usename="
        + usename()
        + ", usesysid="
        + usesysid()
        + ", usecreatedb="
        + usecreatedb()
        + ", usesuper="
        + usesuper()
        + ", usecatupd="
        + usecatupd()
        + ", passwd="
        + passwd()
        + ", valuntil="
        + valuntil()
        + ", useconfig="
        + useconfig()
        + lineSeparator();
  }
}
