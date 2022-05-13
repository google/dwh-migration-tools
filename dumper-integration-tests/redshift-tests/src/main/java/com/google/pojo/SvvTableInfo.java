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
import java.math.BigDecimal;
import java.math.BigInteger;

/** POJO class for serialization data from DB and .csv files. */
public class SvvTableInfo extends DbEntity {

  @CsvBindByName(column = "database")
  private String database;

  @CsvBindByName(column = "schema")
  private String schema;

  @CsvBindByName(column = "table_id")
  private int table_id;

  @CsvBindByName(column = "table")
  private String table;

  @CsvBindByName(column = "encoded")
  private String encoded;

  @CsvBindByName(column = "diststyle")
  private String diststyle;

  @CsvBindByName(column = "sortkey1")
  private String sortkey1;

  @CsvBindByName(column = "max_varchar")
  private int max_varchar;

  @CsvBindByName(column = "sortkey1_enc")
  private String sortkey1_enc;

  @CsvBindByName(column = "sortkey_num")
  private int sortkey_num;

  @CsvBindByName(column = "size")
  private BigInteger size;

  @CsvBindByName(column = "pct_used")
  private BigDecimal pct_used;

  @CsvBindByName(column = "empty")
  private String empty;

  @CsvBindByName(column = "unsorted")
  private BigDecimal unsorted;

  @CsvBindByName(column = "stats_off")
  private BigDecimal stats_off;

  @CsvBindByName(column = "tbl_rows")
  private BigDecimal tbl_rows;

  @CsvBindByName(column = "skew_sortkey1")
  private BigDecimal skew_sortkey1;

  @CsvBindByName(column = "estimated_visible_rows")
  private BigDecimal estimated_visible_rows;

  @CsvBindByName(column = "risk_event")
  private String risk_event;

  @CsvBindByName(column = "vacuum_sort_benefit")
  private BigDecimal vacuum_sort_benefit;

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public int getTable_id() {
    return table_id;
  }

  public void setTable_id(int table_id) {
    this.table_id = table_id;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getEncoded() {
    return encoded;
  }

  public void setEncoded(String encoded) {
    this.encoded = encoded;
  }

  public String getDiststyle() {
    return diststyle;
  }

  public void setDiststyle(String diststyle) {
    this.diststyle = diststyle;
  }

  public String getSortkey1() {
    return sortkey1;
  }

  public void setSortkey1(String sortkey1) {
    this.sortkey1 = sortkey1;
  }

  public int getMax_varchar() {
    return max_varchar;
  }

  public void setMax_varchar(int max_varchar) {
    this.max_varchar = max_varchar;
  }

  public String getSortkey1_enc() {
    return sortkey1_enc;
  }

  public void setSortkey1_enc(String sortkey1_enc) {
    this.sortkey1_enc = sortkey1_enc;
  }

  public int getSortkey_num() {
    return sortkey_num;
  }

  public void setSortkey_num(int sortkey_num) {
    this.sortkey_num = sortkey_num;
  }

  public BigInteger getSize() {
    return size;
  }

  public void setSize(BigInteger size) {
    this.size = size;
  }

  public BigDecimal getPct_used() {
    return pct_used;
  }

  public void setPct_used(BigDecimal pct_used) {
    this.pct_used = pct_used;
  }

  public String getEmpty() {
    return empty;
  }

  public void setEmpty(String empty) {
    this.empty = empty;
  }

  public BigDecimal getUnsorted() {
    return unsorted;
  }

  public void setUnsorted(BigDecimal unsorted) {
    this.unsorted = unsorted;
  }

  public BigDecimal getStats_off() {
    return stats_off;
  }

  public void setStats_off(BigDecimal stats_off) {
    this.stats_off = stats_off;
  }

  public BigDecimal getTbl_rows() {
    return tbl_rows;
  }

  public void setTbl_rows(BigDecimal tbl_rows) {
    this.tbl_rows = tbl_rows;
  }

  public BigDecimal getSkew_sortkey1() {
    return skew_sortkey1;
  }

  public void setSkew_sortkey1(BigDecimal skew_sortkey1) {
    this.skew_sortkey1 = skew_sortkey1;
  }

  public BigDecimal getEstimated_visible_rows() {
    return estimated_visible_rows;
  }

  public void setEstimated_visible_rows(BigDecimal estimated_visible_rows) {
    this.estimated_visible_rows = estimated_visible_rows;
  }

  public String getRisk_event() {
    return risk_event;
  }

  public void setRisk_event(String risk_event) {
    this.risk_event = risk_event;
  }

  public BigDecimal getVacuum_sort_benefit() {
    return vacuum_sort_benefit;
  }

  public void setVacuum_sort_benefit(BigDecimal vacuum_sort_benefit) {
    this.vacuum_sort_benefit = vacuum_sort_benefit;
  }

  @Override
  public String toString() {
    return "SvvTableInfo{"
        + "database='"
        + database
        + '\''
        + ", schema='"
        + schema
        + '\''
        + ", table_id="
        + table_id
        + ", table='"
        + table
        + '\''
        + ", encoded='"
        + encoded
        + '\''
        + ", diststyle='"
        + diststyle
        + '\''
        + ", sortkey1='"
        + sortkey1
        + '\''
        + ", max_varchar="
        + max_varchar
        + ", sortkey1_enc='"
        + sortkey1_enc
        + '\''
        + ", sortkey_num="
        + sortkey_num
        + ", size='"
        + size
        + '\''
        + ", pct_used="
        + pct_used
        + ", empty='"
        + empty
        + '\''
        + ", unsorted="
        + unsorted
        + ", stats_off="
        + stats_off
        + ", tbl_rows="
        + tbl_rows
        + ", skew_sortkey1="
        + skew_sortkey1
        + ", estimated_visible_rows="
        + estimated_visible_rows
        + ", risk_event='"
        + risk_event
        + '\''
        + ", vacuum_sort_benefit="
        + vacuum_sort_benefit
        + '}';
  }
}
