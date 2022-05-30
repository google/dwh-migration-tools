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

import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getIntNotNull;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getStringNotNull;
import static java.lang.System.lineSeparator;

import com.google.auto.value.AutoValue;
import com.google.edwmigration.dumper.csv.CsvUtil;
import java.sql.ResultSet;
import java.sql.SQLException;

/** POJO class for serialization data from DB and CSV files. */
@AutoValue
public abstract class SvvExternalColumnsRow {

  public static SvvExternalColumnsRow create(
      String schemaname,
      String tablename,
      String columnname,
      String external_type,
      int columnnum,
      int part_key,
      String is_nullable) {
    return new AutoValue_SvvExternalColumnsRow(
        schemaname, tablename, columnname, external_type, columnnum, part_key, is_nullable);
  }

  public static SvvExternalColumnsRow create(ResultSet rs) throws SQLException {
    return SvvExternalColumnsRow.create(
        getStringNotNull(rs, "schemaname"),
        getStringNotNull(rs, "tablename"),
        getStringNotNull(rs, "columnname"),
        getStringNotNull(rs, "external_type"),
        getIntNotNull(rs, "columnnum"),
        getIntNotNull(rs, "part_key"),
        getStringNotNull(rs, "is_nullable"));
  }

  public static SvvExternalColumnsRow create(String[] csvLine) {
    return new AutoValue_SvvExternalColumnsRow(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getStringNotNull(csvLine[1]),
        CsvUtil.getStringNotNull(csvLine[2]),
        CsvUtil.getStringNotNull(csvLine[3]),
        CsvUtil.getIntNotNull(csvLine[4]),
        CsvUtil.getIntNotNull(csvLine[5]),
        CsvUtil.getStringNotNull(csvLine[6]));
  }

  public abstract String schemaname();

  public abstract String tablename();

  public abstract String columnname();

  public abstract String externalType();

  public abstract int columnnum();

  public abstract int partKey();

  public abstract String isNullable();

  @Override
  public String toString() {
    return "schemaname="
        + schemaname()
        + ", tablename="
        + tablename()
        + ", columnname"
        + columnname()
        + ", externalType="
        + externalType()
        + ", columnnum="
        + columnnum()
        + ", partKey="
        + partKey()
        + ", isNullable="
        + isNullable()
        + lineSeparator();
  }
}
