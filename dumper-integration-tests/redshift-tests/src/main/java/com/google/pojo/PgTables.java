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
public class PgTables extends DbEntity {

  @CsvBindByName(column = "schemaname")
  private String schemaname;

  @CsvBindByName(column = "tablename")
  private String tablename;

  @CsvBindByName(column = "tableowner")
  private String tableowner;

  @CsvBindByName(column = "tablespace")
  private String tablespace;

  @CsvBindByName(column = "hasindexes")
  private boolean hasindexes;

  @CsvBindByName(column = "hasrules")
  private boolean hasrules;

  @CsvBindByName(column = "hastriggers")
  private boolean hastriggers;

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

  public String getTableowner() {
    return tableowner;
  }

  public void setTableowner(String tableowner) {
    this.tableowner = tableowner;
  }

  public String getTablespace() {
    return tablespace;
  }

  public void setTablespace(String tablespace) {
    this.tablespace = tablespace;
  }

  public boolean isHasindexes() {
    return hasindexes;
  }

  public void setHasindexes(boolean hasindexes) {
    this.hasindexes = hasindexes;
  }

  public boolean isHasrules() {
    return hasrules;
  }

  public void setHasrules(boolean hasrules) {
    this.hasrules = hasrules;
  }

  public boolean isHastriggers() {
    return hastriggers;
  }

  public void setHastriggers(boolean hastriggers) {
    this.hastriggers = hastriggers;
  }

  @Override
  public String toString() {
    return "PgTables{"
        + "schemaname='"
        + schemaname
        + '\''
        + ", tablename='"
        + tablename
        + '\''
        + ", tableowner='"
        + tableowner
        + '\''
        + ", tablespace='"
        + tablespace
        + '\''
        + ", hasindexes="
        + hasindexes
        + ", hasrules="
        + hasrules
        + ", hastriggers="
        + hastriggers
        + '}';
  }
}
