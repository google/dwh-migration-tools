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
public class SvvTables extends DbEntity {

  @CsvBindByName(column = "table_catalog")
  private String table_catalog;

  @CsvBindByName(column = "table_schema")
  private String table_schema;

  @CsvBindByName(column = "table_name")
  private String table_name;

  @CsvBindByName(column = "table_type")
  private String table_type;

  @CsvBindByName(column = "remarks")
  private String remarks;

  public String getTable_catalog() {
    return table_catalog;
  }

  public void setTable_catalog(String table_catalog) {
    this.table_catalog = table_catalog;
  }

  public String getTable_schema() {
    return table_schema;
  }

  public void setTable_schema(String table_schema) {
    this.table_schema = table_schema;
  }

  public String getTable_name() {
    return table_name;
  }

  public void setTable_name(String table_name) {
    this.table_name = table_name;
  }

  public String getTable_type() {
    return table_type;
  }

  public void setTable_type(String table_type) {
    this.table_type = table_type;
  }

  public String getRemarks() {
    return remarks;
  }

  public void setRemarks(String remarks) {
    this.remarks = remarks;
  }

  @Override
  public String toString() {
    return "SvvTables{"
        + "table_catalog='"
        + table_catalog
        + '\''
        + ", table_schema='"
        + table_schema
        + '\''
        + ", table_name='"
        + table_name
        + '\''
        + ", table_type='"
        + table_type
        + '\''
        + ", remarks='"
        + remarks
        + '\''
        + '}';
  }
}
