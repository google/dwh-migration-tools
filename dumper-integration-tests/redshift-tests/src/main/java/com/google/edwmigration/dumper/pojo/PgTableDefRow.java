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
public abstract class PgTableDefRow {

  public static PgTableDefRow create(
      String schemaname,
      String tablename,
      String column,
      String type,
      String encoding,
      boolean distkey,
      int sortkey,
      boolean notnull) {
    return new AutoValue_PgTableDefRow(
        schemaname, tablename, column, type, encoding, distkey, sortkey, notnull);
  }

  public static PgTableDefRow create(ResultSet rs) throws SQLException {
    return PgTableDefRow.create(
        getStringNotNull(rs, "schemaname"),
        getStringNotNull(rs, "tablename"),
        getStringNotNull(rs, "column"),
        getStringNotNull(rs, "type"),
        getStringNotNull(rs, "encoding"),
        getBooleanNotNull(rs, "distkey"),
        getIntNotNull(rs, "sortkey"),
        getBooleanNotNull(rs, "notnull"));
  }

  public static PgTableDefRow create(String[] csvLine) {
    return new AutoValue_PgTableDefRow(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getStringNotNull(csvLine[1]),
        CsvUtil.getStringNotNull(csvLine[2]),
        CsvUtil.getStringNotNull(csvLine[3]),
        CsvUtil.getStringNotNull(csvLine[4]),
        CsvUtil.getBooleanNotNull(csvLine[5]),
        CsvUtil.getIntNotNull(csvLine[6]),
        CsvUtil.getBooleanNotNull(csvLine[7]));
  }

  public abstract String schemaname();

  public abstract String tablename();

  public abstract String column();

  public abstract String type();

  public abstract String encoding();

  public abstract boolean distkey();

  public abstract int sortkey();

  public abstract boolean notnull();

  @Override
  public String toString() {
    return "schemaname="
        + schemaname()
        + ", tablename="
        + tablename()
        + ", column="
        + column()
        + ", type="
        + type()
        + ", encoding="
        + encoding()
        + ", distkey="
        + distkey()
        + ", sortkey="
        + sortkey()
        + ", notnull="
        + notnull()
        + lineSeparator();
  }
}
