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
public class PgUser extends DbEntity {

  @CsvBindByName(column = "usename")
  private String usename;

  @CsvBindByName(column = "usesysid")
  private int usesysid;

  @CsvBindByName(column = "usecreatedb")
  private boolean usecreatedb;

  @CsvBindByName(column = "usesuper")
  private boolean usesuper;

  @CsvBindByName(column = "usecatupd")
  private boolean usecatupd;

  @CsvBindByName(column = "passwd")
  private String passwd;

  @CsvBindByName(column = "valuntil")
  private String valuntil;

  @CsvBindByName(column = "useconfig")
  private String useconfig;

  public String getUsename() {
    return usename;
  }

  public void setUsename(String usename) {
    this.usename = usename;
  }

  public int getUsesysid() {
    return usesysid;
  }

  public void setUsesysid(int usesysid) {
    this.usesysid = usesysid;
  }

  public boolean isUsecreatedb() {
    return usecreatedb;
  }

  public void setUsecreatedb(boolean usecreatedb) {
    this.usecreatedb = usecreatedb;
  }

  public boolean isUsesuper() {
    return usesuper;
  }

  public void setUsesuper(boolean usesuper) {
    this.usesuper = usesuper;
  }

  public boolean isUsecatupd() {
    return usecatupd;
  }

  public void setUsecatupd(boolean usecatupd) {
    this.usecatupd = usecatupd;
  }

  public String getPasswd() {
    return passwd;
  }

  public void setPasswd(String passwd) {
    this.passwd = passwd;
  }

  public String getValuntil() {
    return valuntil;
  }

  public void setValuntil(String valuntil) {
    this.valuntil = valuntil;
  }

  public String getUseconfig() {
    return useconfig;
  }

  public void setUseconfig(String useconfig) {
    this.useconfig = useconfig;
  }

  @Override
  public String toString() {
    return "PgUser{"
        + "usename='"
        + usename
        + '\''
        + ", usesysid="
        + usesysid
        + ", usecreatedb="
        + usecreatedb
        + ", usesuper="
        + usesuper
        + ", usecatupd="
        + usecatupd
        + ", passwd='"
        + passwd
        + '\''
        + ", valuntil='"
        + valuntil
        + '\''
        + ", useconfig='"
        + useconfig
        + '\''
        + '}';
  }
}
