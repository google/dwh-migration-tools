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
public class PgViews extends DbEntity {

  @CsvBindByName(column = "schemaname")
  private String schemaname;

  @CsvBindByName(column = "viewname")
  private String viewname;

  @CsvBindByName(column = "viewowner")
  private String viewowner;

  @CsvBindByName(column = "definition")
  private String definition;

  public String getSchemaname() {
    return schemaname;
  }

  public void setSchemaname(String schemaname) {
    this.schemaname = schemaname;
  }

  public String getViewname() {
    return viewname;
  }

  public void setViewname(String viewname) {
    this.viewname = viewname;
  }

  public String getViewowner() {
    return viewowner;
  }

  public void setViewowner(String viewowner) {
    this.viewowner = viewowner;
  }

  public String getDefinition() {
    return definition;
  }

  public void setDefinition(String definition) {
    this.definition = definition;
  }

  @Override
  public String toString() {
    return "PgViews{"
        + "schemaname='"
        + schemaname
        + '\''
        + ", viewname='"
        + viewname
        + '\''
        + ", viewowner='"
        + viewowner
        + '\''
        + ", definition='"
        + definition
        + '\''
        + '}';
  }
}
