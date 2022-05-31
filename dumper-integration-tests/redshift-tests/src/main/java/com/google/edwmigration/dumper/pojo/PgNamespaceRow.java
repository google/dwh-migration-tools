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
public abstract class PgNamespaceRow {

  public static PgNamespaceRow create(String nspname, int nspowner, String nspacl) {
    return new AutoValue_PgNamespaceRow(nspname, nspowner, nspacl);
  }

  public static PgNamespaceRow create(ResultSet rs) throws SQLException {
    return PgNamespaceRow.create(
        getStringNotNull(rs, "nspname"),
        getIntNotNull(rs, "nspowner"),
        getStringNotNull(rs, "nspacl"));
  }

  public static PgNamespaceRow create(String[] csvLine) {
    return new AutoValue_PgNamespaceRow(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getIntNotNull(csvLine[1]),
        CsvUtil.getStringNotNull(csvLine[2]));
  }

  public abstract String nspname();

  public abstract int nspowner();

  public abstract String nspacl();

  @Override
  public String toString() {
    return "nspname="
        + nspname()
        + ", nspowner="
        + nspowner()
        + ", nspacl="
        + nspacl()
        + lineSeparator();
  }
}
