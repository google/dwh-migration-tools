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
public abstract class PgDatabaseInfoRow {

  public static PgDatabaseInfoRow create(
      String name,
      String owner,
      String encoding,
      String access_privileges) {
    return new AutoValue_PgDatabaseInfoRow(name, owner, encoding, access_privileges);
  }

  public static PgDatabaseInfoRow create(ResultSet rs) throws SQLException {
    return PgDatabaseInfoRow.create(
        getStringNotNull(rs, "name"),
        getStringNotNull(rs, "owner"),
        getStringNotNull(rs, "encoding"),
        getStringNotNull(rs, "access_privileges"));
  }

  public static PgDatabaseInfoRow create(String[] csvLine) {
    return PgDatabaseInfoRow.create(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getStringNotNull(csvLine[1]),
        CsvUtil.getStringNotNull(csvLine[2]),
        CsvUtil.getStringNotNull(csvLine[3]));
  }

  public abstract String name();

  public abstract String owner();

  public abstract String encoding();

  public abstract String accessPrivileges();

  @Override
  public String toString() {
    return "name="
        + name()
        + ", owner="
        + owner()
        + ", encoding="
        + encoding()
        + ", accessPrivileges="
        + accessPrivileges()
        + lineSeparator();
  }

}
