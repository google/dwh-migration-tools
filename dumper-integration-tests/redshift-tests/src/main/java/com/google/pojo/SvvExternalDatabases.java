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
public class SvvExternalDatabases extends DbEntity {

  @CsvBindByName(column = "eskind")
  private int eskind;

  @CsvBindByName(column = "esoptions")
  private String esoptions;

  @CsvBindByName(column = "databasename")
  private String databasename;

  @CsvBindByName(column = "location")
  private String location;

  @CsvBindByName(column = "parameters")
  private String parameters;

  public int getEskind() {
    return eskind;
  }

  public void setEskind(int eskind) {
    this.eskind = eskind;
  }

  public String getEsoptions() {
    return esoptions;
  }

  public void setEsoptions(String esoptions) {
    this.esoptions = esoptions;
  }

  public String getDatabasename() {
    return databasename;
  }

  public void setDatabasename(String databasename) {
    this.databasename = databasename;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public String getParameters() {
    return parameters;
  }

  public void setParameters(String parameters) {
    this.parameters = parameters;
  }

  @Override
  public String toString() {
    return "SvvExternalDatabases{"
        + "eskind="
        + eskind
        + ", esoptions='"
        + esoptions
        + '\''
        + ", databasename='"
        + databasename
        + '\''
        + ", location='"
        + location
        + '\''
        + ", parameters='"
        + parameters
        + '\''
        + '}';
  }
}
