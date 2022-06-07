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
public abstract class CastsRow {

  public static CastsRow create(
      String source_type,
      String target_type,
      String function,
      String implicit) {
    return new AutoValue_CastsRow(source_type, target_type, function, implicit);
  }

  public static CastsRow create(ResultSet rs) throws SQLException {
    return CastsRow.create(
        getStringNotNull(rs, "source_type"),
        getStringNotNull(rs, "target_type"),
        getStringNotNull(rs, "function"),
        getStringNotNull(rs, "implicit"));
  }

  public static CastsRow create(String[] csvLine) {
    return CastsRow.create(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getStringNotNull(csvLine[1]),
        CsvUtil.getStringNotNull(csvLine[2]),
        CsvUtil.getStringNotNull(csvLine[3]));
  }

  public abstract String sourceType();

  public abstract String targetType();

  public abstract String function();

  public abstract String implicit();

  @Override
  public String toString() {
    return "sourceType="
        + sourceType()
        + ", targetType="
        + targetType()
        + ", function="
        + function()
        + ", implicit="
        + implicit()
        + lineSeparator();
  }

}
