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
public abstract class StlDdltextRow {

  public static StlDdltextRow create(
      int userid,
      String xid,
      int pid,
      String label,
      String starttime,
      String endtime,
      int sequence,
      String text) {
    return new AutoValue_StlDdltextRow(userid, xid, pid, label, starttime, endtime, sequence, text);
  }

  public static StlDdltextRow create(ResultSet rs) throws SQLException {
    return StlDdltextRow.create(
        rs.getInt("userid"),
        getStringNotNull(rs, "xid"),
        rs.getInt("pid"),
        getStringNotNull(rs, "label"),
        getStringNotNull(rs, "starttime"),
        getStringNotNull(rs, "endtime"),
        rs.getInt("sequence"),
        getStringNotNull(rs, "text"));
  }

  public static StlDdltextRow create(String[] csvLine) {
    return StlDdltextRow.create(
        CsvUtil.getIntNotNull(csvLine[0]),
        CsvUtil.getStringNotNull(csvLine[1]),
        CsvUtil.getIntNotNull(csvLine[2]),
        CsvUtil.getStringNotNull(csvLine[3]),
        CsvUtil.getStringNotNull(csvLine[4]),
        CsvUtil.getStringNotNull(csvLine[5]),
        CsvUtil.getIntNotNull(csvLine[6]),
        CsvUtil.getStringNotNull(csvLine[7]));
  }

  public abstract int userid();

  public abstract String xid();

  public abstract int pid();

  public abstract String label();

  public abstract String starttime();

  public abstract String endtime();

  public abstract int sequence();

  public abstract String text();

  @Override
  public String toString() {
    return "userid="
        + userid()
        + ", xid="
        + xid()
        + ", pid="
        + pid()
        + ", label="
        + label()
        + ", starttime="
        + starttime()
        + ", endtime="
        + endtime()
        + ", sequence="
        + sequence()
        + ", text="
        + text()
        + lineSeparator();
  }

}
