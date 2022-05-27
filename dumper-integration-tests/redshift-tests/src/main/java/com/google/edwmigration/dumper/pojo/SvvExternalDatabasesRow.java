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
public abstract class SvvExternalDatabasesRow {

  public static SvvExternalDatabasesRow create(
      int eskind, String esoptions, String databasename, String location, String parameters) {
    return new AutoValue_SvvExternalDatabasesRow(
        eskind, esoptions, databasename, location, parameters);
  }

  public static SvvExternalDatabasesRow create(ResultSet rs) throws SQLException {
    return SvvExternalDatabasesRow.create(
        getIntNotNull(rs, "eskind"),
        getStringNotNull(rs, "esoptions"),
        getStringNotNull(rs, "databasename"),
        getStringNotNull(rs, "location"),
        getStringNotNull(rs, "parameters"));
  }

  public static SvvExternalDatabasesRow create(String[] csvLine) {
    return new AutoValue_SvvExternalDatabasesRow(
        CsvUtil.getIntNotNull(csvLine[0]),
        CsvUtil.getStringNotNull(csvLine[1]),
        CsvUtil.getStringNotNull(csvLine[2]),
        CsvUtil.getStringNotNull(csvLine[3]),
        CsvUtil.getStringNotNull(csvLine[4]));
  }

  public abstract int eskind();

  public abstract String esoptions();

  public abstract String databasename();

  public abstract String location();

  public abstract String parameters();

  @Override
  public String toString() {
    return "eskind="
        + eskind()
        + ", esoptions="
        + esoptions()
        + ", databasename="
        + databasename()
        + ", location="
        + location()
        + ", parameters="
        + parameters()
        + lineSeparator();
  }
}
