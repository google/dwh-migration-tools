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
public class PgNamespace extends DbEntity {

  @CsvBindByName(column = "nspname")
  private String nspname;

  @CsvBindByName(column = "nspowner")
  private int nspowner;

  @CsvBindByName(column = "nspacl")
  private String nspacl;

  public String getNspname() {
    return nspname;
  }

  public void setNspname(String nspname) {
    this.nspname = nspname;
  }

  public int getNspowner() {
    return nspowner;
  }

  public void setNspowner(int nspowner) {
    this.nspowner = nspowner;
  }

  public String getNspacl() {
    return nspacl;
  }

  public void setNspacl(String nspacl) {
    this.nspacl = nspacl;
  }

  @Override
  public String toString() {
    return "PgNamespace{"
        + "nspname='"
        + nspname
        + '\''
        + ", nspowner="
        + nspowner
        + ", nspacl='"
        + nspacl
        + '\''
        + '}';
  }
}
