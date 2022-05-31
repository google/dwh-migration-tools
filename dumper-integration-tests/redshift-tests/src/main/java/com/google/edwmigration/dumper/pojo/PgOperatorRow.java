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
public abstract class PgOperatorRow {

  public static PgOperatorRow create(
      String oprname,
      int oprnamespace,
      int oprowner,
      String oprkind,
      boolean oprcanhash,
      int oprleft,
      int oprright,
      int oprresult,
      int oprcom,
      int oprnegate,
      int oprlsortop,
      int oprrsortop,
      int oprltcmpop,
      int oprgtcmpop,
      String oprcode,
      String oprrest,
      String oprjoin) {
    return new AutoValue_PgOperatorRow(
        oprname,
        oprnamespace,
        oprowner,
        oprkind,
        oprcanhash,
        oprleft,
        oprright,
        oprresult,
        oprcom,
        oprnegate,
        oprlsortop,
        oprrsortop,
        oprltcmpop,
        oprgtcmpop,
        oprcode,
        oprrest,
        oprjoin);
  }

  public static PgOperatorRow create(ResultSet rs) throws SQLException {
    return PgOperatorRow.create(
        getStringNotNull(rs, "oprname"),
        getIntNotNull(rs, "oprnamespace"),
        getIntNotNull(rs, "oprowner"),
        getStringNotNull(rs, "oprkind"),
        getBooleanNotNull(rs, "oprcanhash"),
        getIntNotNull(rs, "oprleft"),
        getIntNotNull(rs, "oprright"),
        getIntNotNull(rs, "oprresult"),
        getIntNotNull(rs, "oprcom"),
        getIntNotNull(rs, "oprnegate"),
        getIntNotNull(rs, "oprlsortop"),
        getIntNotNull(rs, "oprrsortop"),
        getIntNotNull(rs, "oprltcmpop"),
        getIntNotNull(rs, "oprgtcmpop"),
        getStringNotNull(rs, "oprcode"),
        getStringNotNull(rs, "oprrest"),
        getStringNotNull(rs, "oprjoin"));
  }

  public static PgOperatorRow create(String[] csvLine) {
    return new AutoValue_PgOperatorRow(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getIntNotNull(csvLine[1]),
        CsvUtil.getIntNotNull(csvLine[2]),
        CsvUtil.getStringNotNull(csvLine[3]),
        CsvUtil.getBooleanNotNull(csvLine[4]),
        CsvUtil.getIntNotNull(csvLine[5]),
        CsvUtil.getIntNotNull(csvLine[6]),
        CsvUtil.getIntNotNull(csvLine[7]),
        CsvUtil.getIntNotNull(csvLine[8]),
        CsvUtil.getIntNotNull(csvLine[9]),
        CsvUtil.getIntNotNull(csvLine[10]),
        CsvUtil.getIntNotNull(csvLine[11]),
        CsvUtil.getIntNotNull(csvLine[12]),
        CsvUtil.getIntNotNull(csvLine[13]),
        CsvUtil.getStringNotNull(csvLine[14]),
        CsvUtil.getStringNotNull(csvLine[15]),
        CsvUtil.getStringNotNull(csvLine[16]));
  }

  public abstract String oprname();

  public abstract int oprnamespace();

  public abstract int oprowner();

  public abstract String oprkind();

  public abstract boolean oprcanhash();

  public abstract int oprleft();

  public abstract int oprright();

  public abstract int oprresult();

  public abstract int oprcom();

  public abstract int oprnegate();

  public abstract int oprlsortop();

  public abstract int oprrsortop();

  public abstract int oprltcmpop();

  public abstract int oprgtcmpop();

  public abstract String oprcode();

  public abstract String oprrest();

  public abstract String oprjoin();

  @Override
  public String toString() {
    return "oprname="
        + oprname()
        + ", oprnamespace="
        + oprnamespace()
        + ", oprowner="
        + oprowner()
        + ", oprkind="
        + oprkind()
        + ", oprcanhash="
        + oprcanhash()
        + ", oprleft="
        + oprleft()
        + ", oprright="
        + oprright()
        + ", oprresult="
        + oprresult()
        + ", oprcom="
        + oprcom()
        + ", oprnegate="
        + oprnegate()
        + ", oprlsortop="
        + oprlsortop()
        + ", oprrsortop="
        + oprrsortop()
        + ", oprltcmpop="
        + oprltcmpop()
        + ", oprgtcmpop="
        + oprgtcmpop()
        + ", oprcode="
        + oprcode()
        + ", oprrest="
        + oprrest()
        + ", oprjoin="
        + oprjoin()
        + lineSeparator();
  }
}
