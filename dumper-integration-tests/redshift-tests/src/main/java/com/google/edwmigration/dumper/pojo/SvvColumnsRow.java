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
public abstract class SvvColumnsRow {

  public static SvvColumnsRow create(
      String tableCatalog,
      String tableSchema,
      String tableName,
      String columnName,
      int ordinalPosition,
      String columnDefault,
      String isNullable,
      String dataType,
      int characterMaximumLength,
      int numericPrecision,
      int numericPrecisionRadix,
      int numericScale,
      int datetimePrecision,
      String intervalType,
      String intervalPrecision,
      String characterSetCatalog,
      String characterSetSchema,
      String characterSetName,
      String collationCatalog,
      String collationSchema,
      String collationName,
      String domainName,
      String remarks) {
    return new AutoValue_SvvColumnsRow(
        tableCatalog,
        tableSchema,
        tableName,
        columnName,
        ordinalPosition,
        columnDefault,
        isNullable,
        dataType,
        characterMaximumLength,
        numericPrecision,
        numericPrecisionRadix,
        numericScale,
        datetimePrecision,
        intervalType,
        intervalPrecision,
        characterSetCatalog,
        characterSetSchema,
        characterSetName,
        collationCatalog,
        collationSchema,
        collationName,
        domainName,
        remarks);
  }

  public static SvvColumnsRow create(ResultSet rs) throws SQLException {
    return SvvColumnsRow.create(
        getStringNotNull(rs, "table_catalog"),
        getStringNotNull(rs, "table_schema"),
        getStringNotNull(rs, "table_name"),
        getStringNotNull(rs, "column_name"),
        getIntNotNull(rs, "ordinal_position"),
        getStringNotNull(rs, "column_default"),
        getStringNotNull(rs, "is_nullable"),
        getStringNotNull(rs, "data_type"),
        getIntNotNull(rs, "character_maximum_length"),
        getIntNotNull(rs, "numeric_precision"),
        getIntNotNull(rs, "numeric_precision_radix"),
        getIntNotNull(rs, "numeric_scale"),
        getIntNotNull(rs, "datetime_precision"),
        getStringNotNull(rs, "interval_type"),
        getStringNotNull(rs, "interval_precision"),
        getStringNotNull(rs, "character_set_catalog"),
        getStringNotNull(rs, "character_set_schema"),
        getStringNotNull(rs, "character_set_name"),
        getStringNotNull(rs, "collation_catalog"),
        getStringNotNull(rs, "collation_schema"),
        getStringNotNull(rs, "collation_name"),
        getStringNotNull(rs, "domain_name"),
        getStringNotNull(rs, "remarks"));
  }

  public static SvvColumnsRow create(String[] csvLine) {
    return new AutoValue_SvvColumnsRow(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getStringNotNull(csvLine[1]),
        CsvUtil.getStringNotNull(csvLine[2]),
        CsvUtil.getStringNotNull(csvLine[3]),
        CsvUtil.getIntNotNull(csvLine[4]),
        CsvUtil.getStringNotNull(csvLine[5]),
        CsvUtil.getStringNotNull(csvLine[6]),
        CsvUtil.getStringNotNull(csvLine[7]),
        CsvUtil.getIntNotNull(csvLine[8]),
        CsvUtil.getIntNotNull(csvLine[9]),
        CsvUtil.getIntNotNull(csvLine[10]),
        CsvUtil.getIntNotNull(csvLine[11]),
        CsvUtil.getIntNotNull(csvLine[12]),
        CsvUtil.getStringNotNull(csvLine[13]),
        CsvUtil.getStringNotNull(csvLine[14]),
        CsvUtil.getStringNotNull(csvLine[15]),
        CsvUtil.getStringNotNull(csvLine[16]),
        CsvUtil.getStringNotNull(csvLine[17]),
        CsvUtil.getStringNotNull(csvLine[18]),
        CsvUtil.getStringNotNull(csvLine[19]),
        CsvUtil.getStringNotNull(csvLine[20]),
        CsvUtil.getStringNotNull(csvLine[21]),
        CsvUtil.getStringNotNull(csvLine[22]));
  }

  public abstract String tableCatalog();

  public abstract String tableSchema();

  public abstract String tableName();

  public abstract String columnName();

  public abstract int ordinalPosition();

  public abstract String columnDefault();

  public abstract String isNullable();

  public abstract String dataType();

  public abstract int characterMaximumLength();

  public abstract int numericPrecision();

  public abstract int numericPrecisionRadix();

  public abstract int numericScale();

  public abstract int datetimePrecision();

  public abstract String intervalType();

  public abstract String intervalPrecision();

  public abstract String characterSetCatalog();

  public abstract String characterSetSchema();

  public abstract String characterSetName();

  public abstract String collationCatalog();

  public abstract String collationSchema();

  public abstract String collationName();

  public abstract String domainName();

  public abstract String remarks();

  @Override
  public String toString() {
    return "tableCatalog="
        + tableCatalog()
        + ", tableSchema="
        + tableSchema()
        + ", tableName="
        + tableName()
        + ", columnName="
        + columnName()
        + ", ordinalPosition="
        + ordinalPosition()
        + ", columnDefault="
        + columnDefault()
        + ", isNullable="
        + isNullable()
        + ", dataType="
        + dataType()
        + ", characterMaximumLength="
        + characterMaximumLength()
        + ", numericPrecision="
        + numericPrecision()
        + ", numericPrecisionRadix="
        + numericPrecisionRadix()
        + ", numericScale="
        + numericScale()
        + ", datetimePrecision="
        + datetimePrecision()
        + ", intervalType="
        + intervalType()
        + ", intervalPrecision="
        + intervalPrecision()
        + ", characterSetCatalog="
        + characterSetCatalog()
        + ", characterSetSchema="
        + characterSetSchema()
        + ", characterSetName="
        + characterSetName()
        + ", collationCatalog="
        + collationCatalog()
        + ", collationSchema="
        + collationSchema()
        + ", collationName="
        + collationName()
        + ", domainName="
        + domainName()
        + ", remarks="
        + remarks()
        + lineSeparator();
  }
}
