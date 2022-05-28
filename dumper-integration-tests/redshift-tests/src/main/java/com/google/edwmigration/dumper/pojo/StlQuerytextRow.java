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

import static com.google.edwmigration.dumper.csv.CsvUtil.getIntNotNull;
import static com.google.edwmigration.dumper.csv.CsvUtil.getStringNotNull;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getIntNotNull;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getStringNotNull;
import static java.lang.System.lineSeparator;

import com.google.auto.value.AutoValue;
import java.sql.ResultSet;
import java.sql.SQLException;

/** POJO class for serialization data from DB and CSV files. */
@AutoValue
public abstract class StlQuerytextRow {

  public static StlQuerytextRow create(int sequence, String text) {
    return new AutoValue_StlQuerytextRow(sequence, text);
  }

  public static StlQuerytextRow create(ResultSet rs) throws SQLException {
    return StlQuerytextRow.create(getIntNotNull(rs, "sequence"), getStringNotNull(rs, "text"));
  }

  public static StlQuerytextRow create(String[] csvLine) {
    return StlQuerytextRow.create(getIntNotNull(csvLine[0]), getStringNotNull(csvLine[1]));
  }

  public abstract int sequence();

  public abstract String text();

  @Override
  public String toString() {
    return "sequence=" + sequence() + ", text=" + text() + lineSeparator();
  }
}
