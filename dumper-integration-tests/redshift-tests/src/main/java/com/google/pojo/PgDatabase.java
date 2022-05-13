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
public class PgDatabase extends DbEntity {

  @CsvBindByName(column = "datname")
  private String datname;

  @CsvBindByName(column = "datdba")
  private int datdba;

  @CsvBindByName(column = "encoding")
  private int encoding;

  @CsvBindByName(column = "datistemplate")
  private boolean datistemplate;

  @CsvBindByName(column = "datlastsysoid")
  private int datlastsysoid;

  @CsvBindByName(column = "datvacuumxid")
  private int datvacuumxid;

  @CsvBindByName(column = "datfrozenxid")
  private int datfrozenxid;

  @CsvBindByName(column = "dattablespace")
  private int dattablespace;

  @CsvBindByName(column = "datconfig")
  private int datconfig;

  @CsvBindByName(column = "datacl")
  private String datacl;

  public String getDatname() {
    return datname;
  }

  public void setDatname(String datname) {
    this.datname = datname;
  }

  public int getDatdba() {
    return datdba;
  }

  public void setDatdba(int datdba) {
    this.datdba = datdba;
  }

  public int getEncoding() {
    return encoding;
  }

  public void setEncoding(int encoding) {
    this.encoding = encoding;
  }

  public boolean isDatistemplate() {
    return datistemplate;
  }

  public void setDatistemplate(boolean datistemplate) {
    this.datistemplate = datistemplate;
  }

  public int getDatlastsysoid() {
    return datlastsysoid;
  }

  public void setDatlastsysoid(int datlastsysoid) {
    this.datlastsysoid = datlastsysoid;
  }

  public int getDatvacuumxid() {
    return datvacuumxid;
  }

  public void setDatvacuumxid(int datvacuumxid) {
    this.datvacuumxid = datvacuumxid;
  }

  public int getDatfrozenxid() {
    return datfrozenxid;
  }

  public void setDatfrozenxid(int datfrozenxid) {
    this.datfrozenxid = datfrozenxid;
  }

  public int getDattablespace() {
    return dattablespace;
  }

  public void setDattablespace(int dattablespace) {
    this.dattablespace = dattablespace;
  }

  public int getDatconfig() {
    return datconfig;
  }

  public void setDatconfig(int datconfig) {
    this.datconfig = datconfig;
  }

  public String getDatacl() {
    return datacl;
  }

  public void setDatacl(String datacl) {
    this.datacl = datacl;
  }

  @Override
  public String toString() {
    return "PgDatabase{"
        + "datname='"
        + datname
        + '\''
        + ", datdba="
        + datdba
        + ", encoding="
        + encoding
        + ", datistemplate="
        + datistemplate
        + ", datlastsysoid="
        + datlastsysoid
        + ", datvacuumxid="
        + datvacuumxid
        + ", datfrozenxid="
        + datfrozenxid
        + ", dattablespace="
        + dattablespace
        + ", datconfig="
        + datconfig
        + ", datacl='"
        + datacl
        + '\''
        + '}';
  }
}
