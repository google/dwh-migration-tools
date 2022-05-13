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
public class PgOperator extends DbEntity {

  @CsvBindByName(column = "oprname")
  private String oprname;

  @CsvBindByName(column = "oprnamespace")
  private int oprnamespace;

  @CsvBindByName(column = "oprowner")
  private int oprowner;

  @CsvBindByName(column = "oprkind")
  private String oprkind;

  @CsvBindByName(column = "oprcanhash")
  private boolean oprcanhash;

  @CsvBindByName(column = "oprleft")
  private int oprleft;

  @CsvBindByName(column = "oprright")
  private int oprright;

  @CsvBindByName(column = "oprresult")
  private int oprresult;

  @CsvBindByName(column = "oprcom")
  private int oprcom;

  @CsvBindByName(column = "oprnegate")
  private int oprnegate;

  @CsvBindByName(column = "oprlsortop")
  private int oprlsortop;

  @CsvBindByName(column = "oprrsortop")
  private int oprrsortop;

  @CsvBindByName(column = "oprltcmpop")
  private int oprltcmpop;

  @CsvBindByName(column = "oprgtcmpop")
  private int oprgtcmpop;

  @CsvBindByName(column = "oprcode")
  private String oprcode;

  @CsvBindByName(column = "oprrest")
  private String oprrest;

  @CsvBindByName(column = "oprjoin")
  private String oprjoin;

  public String getOprname() {
    return oprname;
  }

  public void setOprname(String oprname) {
    this.oprname = oprname;
  }

  public int getOprnamespace() {
    return oprnamespace;
  }

  public void setOprnamespace(int oprnamespace) {
    this.oprnamespace = oprnamespace;
  }

  public int getOprowner() {
    return oprowner;
  }

  public void setOprowner(int oprowner) {
    this.oprowner = oprowner;
  }

  public String getOprkind() {
    return oprkind;
  }

  public void setOprkind(String oprkind) {
    this.oprkind = oprkind;
  }

  public boolean isOprcanhash() {
    return oprcanhash;
  }

  public void setOprcanhash(boolean oprcanhash) {
    this.oprcanhash = oprcanhash;
  }

  public int getOprleft() {
    return oprleft;
  }

  public void setOprleft(int oprleft) {
    this.oprleft = oprleft;
  }

  public int getOprright() {
    return oprright;
  }

  public void setOprright(int oprright) {
    this.oprright = oprright;
  }

  public int getOprresult() {
    return oprresult;
  }

  public void setOprresult(int oprresult) {
    this.oprresult = oprresult;
  }

  public int getOprcom() {
    return oprcom;
  }

  public void setOprcom(int oprcom) {
    this.oprcom = oprcom;
  }

  public int getOprnegate() {
    return oprnegate;
  }

  public void setOprnegate(int oprnegate) {
    this.oprnegate = oprnegate;
  }

  public int getOprlsortop() {
    return oprlsortop;
  }

  public void setOprlsortop(int oprlsortop) {
    this.oprlsortop = oprlsortop;
  }

  public int getOprrsortop() {
    return oprrsortop;
  }

  public void setOprrsortop(int oprrsortop) {
    this.oprrsortop = oprrsortop;
  }

  public int getOprltcmpop() {
    return oprltcmpop;
  }

  public void setOprltcmpop(int oprltcmpop) {
    this.oprltcmpop = oprltcmpop;
  }

  public int getOprgtcmpop() {
    return oprgtcmpop;
  }

  public void setOprgtcmpop(int oprgtcmpop) {
    this.oprgtcmpop = oprgtcmpop;
  }

  public String getOprcode() {
    return oprcode;
  }

  public void setOprcode(String oprcode) {
    this.oprcode = oprcode;
  }

  public String getOprrest() {
    return oprrest;
  }

  public void setOprrest(String oprrest) {
    this.oprrest = oprrest;
  }

  public String getOprjoin() {
    return oprjoin;
  }

  public void setOprjoin(String oprjoin) {
    this.oprjoin = oprjoin;
  }

  @Override
  public String toString() {
    return "PgOperator{"
        + "oprname='"
        + oprname
        + '\''
        + ", oprnamespace="
        + oprnamespace
        + ", oprowner="
        + oprowner
        + ", oprkind='"
        + oprkind
        + '\''
        + ", oprcanhash="
        + oprcanhash
        + ", oprleft="
        + oprleft
        + ", oprright="
        + oprright
        + ", oprresult="
        + oprresult
        + ", oprcom="
        + oprcom
        + ", oprnegate="
        + oprnegate
        + ", oprlsortop="
        + oprlsortop
        + ", oprrsortop="
        + oprrsortop
        + ", oprltcmpop="
        + oprltcmpop
        + ", oprgtcmpop="
        + oprgtcmpop
        + ", oprcode='"
        + oprcode
        + '\''
        + ", oprrest='"
        + oprrest
        + '\''
        + ", oprjoin='"
        + oprjoin
        + '\''
        + '}';
  }
}
