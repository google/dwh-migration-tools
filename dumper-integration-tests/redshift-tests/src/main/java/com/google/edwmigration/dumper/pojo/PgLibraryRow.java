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
public abstract class PgLibraryRow {

  public static PgLibraryRow create(
      String name,
      int language_oid,
      int file_store_id,
      int owner) {
    return new AutoValue_PgLibraryRow(name, language_oid, file_store_id, owner);
  }

  public static PgLibraryRow create(ResultSet rs) throws SQLException {
    return PgLibraryRow.create(
        getStringNotNull(rs, "name"),
        rs.getInt("language_oid"),
        rs.getInt("file_store_id"),
        rs.getInt("owner"));
  }

  public static PgLibraryRow create(String[] csvLine) {
    return PgLibraryRow.create(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getIntNotNull(csvLine[1]),
        CsvUtil.getIntNotNull(csvLine[2]),
        CsvUtil.getIntNotNull(csvLine[3]));
  }

  public abstract String name();

  public abstract int languageOid();

  public abstract int fileStoreId();

  public abstract int owner();

  @Override
  public String toString() {
    return "name="
        + name()
        + ", languageOid="
        + languageOid()
        + ", fileStoreId="
        + fileStoreId()
        + ", owner="
        + owner()
        + lineSeparator();
  }
}
