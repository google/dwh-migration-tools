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

import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getBigDecimalNotNull;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getBigIntegerNotNull;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getIntNotNull;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getStringNotNull;
import static java.lang.System.lineSeparator;

import com.google.auto.value.AutoValue;
import com.google.edwmigration.dumper.csv.CsvUtil;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

@AutoValue
public abstract class SvvTableInfoRow {

  public static SvvTableInfoRow create(
      String database,
      String schema,
      int table_id,
      String table,
      String encoded,
      String diststyle,
      String sortkey1,
      int max_varchar,
      String sortkey1_enc,
      int sortkey_num,
      BigInteger size,
      BigDecimal pct_used,
      BigDecimal empty,
      BigDecimal unsorted,
      BigDecimal stats_off,
      BigDecimal tbl_rows,
      BigDecimal skew_sortkey1,
      BigDecimal skew_rows,
      BigDecimal estimated_visible_rows,
      String risk_event,
      BigDecimal vacuum_sort_benefit) {
    return new AutoValue_SvvTableInfoRow(
        database,
        schema,
        table_id,
        table,
        encoded,
        diststyle,
        sortkey1,
        max_varchar,
        sortkey1_enc,
        sortkey_num,
        size,
        pct_used,
        empty,
        unsorted,
        stats_off,
        tbl_rows,
        skew_sortkey1,
        skew_rows,
        estimated_visible_rows,
        risk_event,
        vacuum_sort_benefit);
  }

  public static SvvTableInfoRow create(ResultSet rs) throws SQLException {
    return SvvTableInfoRow.create(
        getStringNotNull(rs, "database"),
        getStringNotNull(rs, "schema"),
        getIntNotNull(rs, "table_id"),
        getStringNotNull(rs, "table"),
        getStringNotNull(rs, "encoded"),
        getStringNotNull(rs, "diststyle"),
        getStringNotNull(rs, "sortkey1"),
        getIntNotNull(rs, "max_varchar"),
        getStringNotNull(rs, "sortkey1_enc"),
        getIntNotNull(rs, "sortkey_num"),
        getBigIntegerNotNull(rs, "size"),
        getBigDecimalNotNull(rs, "pct_used"),
        getBigDecimalNotNull(rs, "empty"),
        getBigDecimalNotNull(rs, "unsorted"),
        getBigDecimalNotNull(rs, "stats_off"),
        getBigDecimalNotNull(rs, "tbl_rows"),
        getBigDecimalNotNull(rs, "skew_sortkey1"),
        getBigDecimalNotNull(rs, "skew_rows"),
        getBigDecimalNotNull(rs, "estimated_visible_rows"),
        getStringNotNull(rs, "risk_event"),
        getBigDecimalNotNull(rs, "vacuum_sort_benefit"));
  }

  public static SvvTableInfoRow create(String[] csvLine) {
    return new AutoValue_SvvTableInfoRow(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getStringNotNull(csvLine[1]),
        CsvUtil.getIntNotNull(csvLine[2]),
        CsvUtil.getStringNotNull(csvLine[3]),
        CsvUtil.getStringNotNull(csvLine[4]),
        CsvUtil.getStringNotNull(csvLine[5]),
        CsvUtil.getStringNotNull(csvLine[6]),
        CsvUtil.getIntNotNull(csvLine[7]),
        CsvUtil.getStringNotNull(csvLine[8]),
        CsvUtil.getIntNotNull(csvLine[9]),
        CsvUtil.getBigIntegerNotNull(csvLine[10]),
        CsvUtil.getBigDecimalNotNull(csvLine[11]),
        CsvUtil.getBigDecimalNotNull(csvLine[12]),
        CsvUtil.getBigDecimalNotNull(csvLine[13]),
        CsvUtil.getBigDecimalNotNull(csvLine[14]),
        CsvUtil.getBigDecimalNotNull(csvLine[15]),
        CsvUtil.getBigDecimalNotNull(csvLine[16]),
        CsvUtil.getBigDecimalNotNull(csvLine[17]),
        CsvUtil.getBigDecimalNotNull(csvLine[18]),
        CsvUtil.getStringNotNull(csvLine[19]),
        CsvUtil.getBigDecimalNotNull(csvLine[20]));
  }

  public abstract String database();

  public abstract String schema();

  public abstract int tableId();

  public abstract String table();

  public abstract String encoded();

  public abstract String diststyle();

  public abstract String sortkey1();

  public abstract int maxVarchar();

  public abstract String sortkey1Enc();

  public abstract int sortkeyNum();

  public abstract BigInteger size();

  public abstract BigDecimal pctUsed();

  public abstract BigDecimal empty();

  public abstract BigDecimal unsorted();

  public abstract BigDecimal statsOff();

  public abstract BigDecimal tblRows();

  public abstract BigDecimal skewSortkey1();

  public abstract BigDecimal skewRows();

  public abstract BigDecimal estimatedVisibleRows();

  public abstract String riskEvent();

  public abstract BigDecimal vacuumSortBenefit();

  @Override
  public String toString() {
    return "database="
        + database()
        + ", schema="
        + schema()
        + ", tableId="
        + tableId()
        + ", table="
        + table()
        + ", encoded="
        + encoded()
        + ", encoded="
        + diststyle()
        + ", sortkey1="
        + sortkey1()
        + ", maxVarchar="
        + maxVarchar()
        + ", sortkey1Enc= "
        + sortkey1Enc()
        + ", sortkeyNum="
        + sortkeyNum()
        + ", size="
        + size()
        + ", pctUsed="
        + pctUsed()
        + ", empty="
        + empty()
        + ", unsorted="
        + unsorted()
        + ", statsOff="
        + statsOff()
        + ", tblRows="
        + tblRows()
        + ", skewSortkey1="
        + skewSortkey1()
        + ", skewRows="
        + skewRows()
        + ", estimatedVisibleRows="
        + estimatedVisibleRows()
        + ", riskEvent="
        + riskEvent()
        + ", vacuumSortBenefit="
        + vacuumSortBenefit()
        + lineSeparator();
  }
}
