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
public class SvvExternalSchemas extends DbEntity {

  @CsvBindByName(column = "esoid")
  private int esoid;

  @CsvBindByName(column = "eskind")
  private int eskind;

  @CsvBindByName(column = "schemaname")
  private String schemaname;

  @CsvBindByName(column = "esowner")
  private int esowner;

  @CsvBindByName(column = "databasename")
  private String databasename;

  @CsvBindByName(column = "esoptions")
  private String esoptions;

  public int getEsoid() {
    return esoid;
  }

  public void setEsoid(int esoid) {
    this.esoid = esoid;
  }

  public int getEskind() {
    return eskind;
  }

  public void setEskind(int eskind) {
    this.eskind = eskind;
  }

  public String getSchemaname() {
    return schemaname;
  }

  public void setSchemaname(String schemaname) {
    this.schemaname = schemaname;
  }

  public int getEsowner() {
    return esowner;
  }

  public void setEsowner(int esowner) {
    this.esowner = esowner;
  }

  public String getDatabasename() {
    return databasename;
  }

  public void setDatabasename(String databasename) {
    this.databasename = databasename;
  }

  public String getEsoptions() {
    return esoptions;
  }

  public void setEsoptions(String esoptions) {
    this.esoptions = esoptions;
  }

  @Override
  public String toString() {
    return "SvvExternalSchemas{"
        + "esoid="
        + esoid
        + ", eskind="
        + eskind
        + ", schemaname='"
        + schemaname
        + '\''
        + ", esowner="
        + esowner
        + ", databasename='"
        + databasename
        + '\''
        + ", esoptions='"
        + esoptions
        + '\''
        + '}';
  }
}
