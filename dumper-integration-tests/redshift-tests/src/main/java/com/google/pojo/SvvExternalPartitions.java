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
public class SvvExternalPartitions extends DbEntity {

  @CsvBindByName(column = "schemaname")
  private String schemaname;

  @CsvBindByName(column = "tablename")
  private String tablename;

  @CsvBindByName(column = "values")
  private String values;

  @CsvBindByName(column = "location")
  private String location;

  @CsvBindByName(column = "input_format")
  private String input_format;

  @CsvBindByName(column = "output_format")
  private String output_format;

  @CsvBindByName(column = "serialization_lib")
  private String serialization_lib;

  @CsvBindByName(column = "serde_parameters")
  private String serde_parameters;

  @CsvBindByName(column = "compressed")
  private int compressed;

  @CsvBindByName(column = "parameters")
  private String parameters;

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

  public String getValues() {
    return values;
  }

  public void setValues(String values) {
    this.values = values;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public String getInput_format() {
    return input_format;
  }

  public void setInput_format(String input_format) {
    this.input_format = input_format;
  }

  public String getOutput_format() {
    return output_format;
  }

  public void setOutput_format(String output_format) {
    this.output_format = output_format;
  }

  public String getSerialization_lib() {
    return serialization_lib;
  }

  public void setSerialization_lib(String serialization_lib) {
    this.serialization_lib = serialization_lib;
  }

  public String getSerde_parameters() {
    return serde_parameters;
  }

  public void setSerde_parameters(String serde_parameters) {
    this.serde_parameters = serde_parameters;
  }

  public int getCompressed() {
    return compressed;
  }

  public void setCompressed(int compressed) {
    this.compressed = compressed;
  }

  public String getParameters() {
    return parameters;
  }

  public void setParameters(String parameters) {
    this.parameters = parameters;
  }

  @Override
  public String toString() {
    return "SvvExternalPartitions{"
        + "schemaname='"
        + schemaname
        + '\''
        + ", tablename='"
        + tablename
        + '\''
        + ", values='"
        + values
        + '\''
        + ", location='"
        + location
        + '\''
        + ", input_format='"
        + input_format
        + '\''
        + ", output_format='"
        + output_format
        + '\''
        + ", serialization_lib='"
        + serialization_lib
        + '\''
        + ", serde_parameters='"
        + serde_parameters
        + '\''
        + ", compressed="
        + compressed
        + ", parameters='"
        + parameters
        + '\''
        + '}';
  }
}
