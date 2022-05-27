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
public abstract class SvvExternalTablesRow {

  public static SvvExternalTablesRow create(
      String schemaname,
      String tablename,
      String location,
      String input_format,
      String output_format,
      String serialization_lib,
      String serde_parameters,
      int compressed,
      String parameters,
      String tabletype) {
    return new AutoValue_SvvExternalTablesRow(
        schemaname,
        tablename,
        location,
        input_format,
        output_format,
        serialization_lib,
        serde_parameters,
        compressed,
        parameters,
        tabletype);
  }

  public static SvvExternalTablesRow create(ResultSet rs) throws SQLException {
    return SvvExternalTablesRow.create(
        getStringNotNull(rs, "schemaname"),
        getStringNotNull(rs, "tablename"),
        getStringNotNull(rs, "location"),
        getStringNotNull(rs, "input_format"),
        getStringNotNull(rs, "output_format"),
        getStringNotNull(rs, "serialization_lib"),
        getStringNotNull(rs, "serde_parameters"),
        getIntNotNull(rs, "compressed"),
        getStringNotNull(rs, "parameters"),
        getStringNotNull(rs, "tabletype"));
  }

  public static SvvExternalTablesRow create(String[] csvLine) {
    return new AutoValue_SvvExternalTablesRow(
        CsvUtil.getStringNotNull(csvLine[0]),
        CsvUtil.getStringNotNull(csvLine[1]),
        CsvUtil.getStringNotNull(csvLine[2]),
        CsvUtil.getStringNotNull(csvLine[3]),
        CsvUtil.getStringNotNull(csvLine[4]),
        CsvUtil.getStringNotNull(csvLine[5]),
        CsvUtil.getStringNotNull(csvLine[6]),
        CsvUtil.getIntNotNull(csvLine[7]),
        CsvUtil.getStringNotNull(csvLine[8]),
        CsvUtil.getStringNotNull(csvLine[9]));
  }

  public abstract String schemaname();

  public abstract String tablename();

  public abstract String location();

  public abstract String inputFormat();

  public abstract String outputFormat();

  public abstract String serializationLib();

  public abstract String serdeParameters();

  public abstract int compressed();

  public abstract String parameters();

  public abstract String tabletype();

  @Override
  public String toString() {
    return "schemaname="
        + schemaname()
        + ", tablename="
        + tablename()
        + ", location="
        + location()
        + ", inputFormat="
        + inputFormat()
        + ", outputFormat="
        + outputFormat()
        + ", serializationLib="
        + serializationLib()
        + ", serdeParameters="
        + serdeParameters()
        + ", compressed="
        + compressed()
        + ", parameters="
        + parameters()
        + ", tabletype="
        + tabletype()
        + lineSeparator();
  }
}
