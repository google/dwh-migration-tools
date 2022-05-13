/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.pojo;

import com.opencsv.bean.CsvBindByName;

/** POJO class for serialization data from DB and .csv files. */
public class SvvExternalColumns extends DbEntity {

  @CsvBindByName(column = "schemaname")
  private String schemaname;

  @CsvBindByName(column = "tablename")
  private String tablename;

  @CsvBindByName(column = "columnname")
  private String columnname;

  @CsvBindByName(column = "external_type")
  private String external_type;

  @CsvBindByName(column = "columnnum")
  private int columnnum;

  @CsvBindByName(column = "part_key")
  private int part_key;

  @CsvBindByName(column = "is_nullable")
  private String is_nullable;

  public String getSchemaname() {
    return schemaname;
  }

  public void setSchemaname(String schemaname) {
    this.schemaname = schemaname;
  }

  public String getTablename() {
    return tablename;
  }

  public void setTablename(String tablename) {
    this.tablename = tablename;
  }

  public String getColumnname() {
    return columnname;
  }

  public void setColumnname(String columnname) {
    this.columnname = columnname;
  }

  public String getExternal_type() {
    return external_type;
  }

  public void setExternal_type(String external_type) {
    this.external_type = external_type;
  }

  public int getColumnnum() {
    return columnnum;
  }

  public void setColumnnum(int columnnum) {
    this.columnnum = columnnum;
  }

  public int getPart_key() {
    return part_key;
  }

  public void setPart_key(int part_key) {
    this.part_key = part_key;
  }

  public String getIs_nullable() {
    return is_nullable;
  }

  public void setIs_nullable(String is_nullable) {
    this.is_nullable = is_nullable;
  }

  @Override
  public String toString() {
    return "SvvExternalColumns{"
        + "schemaname='"
        + schemaname
        + '\''
        + ", tablename='"
        + tablename
        + '\''
        + ", columnname='"
        + columnname
        + '\''
        + ", external_type='"
        + external_type
        + '\''
        + ", columnnum="
        + columnnum
        + ", part_key="
        + part_key
        + ", is_nullable='"
        + is_nullable
        + '\''
        + '}';
  }
}
