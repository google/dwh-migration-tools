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
public class PgTableDef extends DbEntity {

  @CsvBindByName(column = "schemaname")
  private String schemaname;

  @CsvBindByName(column = "tablename")
  private String tablename;

  @CsvBindByName(column = "column")
  private String column;

  @CsvBindByName(column = "type")
  private String type;

  @CsvBindByName(column = "encoding")
  private String encoding;

  @CsvBindByName(column = "distkey")
  private boolean distkey;

  @CsvBindByName(column = "sortkey")
  private int sortkey;

  @CsvBindByName(column = "notnull")
  private boolean notnull;

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

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  public boolean isDistkey() {
    return distkey;
  }

  public void setDistkey(boolean distkey) {
    this.distkey = distkey;
  }

  public int getSortkey() {
    return sortkey;
  }

  public void setSortkey(int sortkey) {
    this.sortkey = sortkey;
  }

  public boolean isNotnull() {
    return notnull;
  }

  public void setNotnull(boolean notnull) {
    this.notnull = notnull;
  }

  @Override
  public String toString() {
    return "PgTableDef{"
        + "schemaname='"
        + schemaname
        + '\''
        + ", tablename='"
        + tablename
        + '\''
        + ", column='"
        + column
        + '\''
        + ", type='"
        + type
        + '\''
        + ", encoding='"
        + encoding
        + '\''
        + ", distkey="
        + distkey
        + ", sortkey="
        + sortkey
        + ", notnull="
        + notnull
        + '}';
  }
}
