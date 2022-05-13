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
public class PgLibrary extends DbEntity {

  @CsvBindByName(column = "name")
  private String name;

  @CsvBindByName(column = "language_oid")
  private int language_oid;

  @CsvBindByName(column = "file_store_id")
  private int file_store_id;

  @CsvBindByName(column = "owner")
  private int owner;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getLanguage_oid() {
    return language_oid;
  }

  public void setLanguage_oid(int language_oid) {
    this.language_oid = language_oid;
  }

  public int getFile_store_id() {
    return file_store_id;
  }

  public void setFile_store_id(int file_store_id) {
    this.file_store_id = file_store_id;
  }

  public int getOwner() {
    return owner;
  }

  public void setOwner(int owner) {
    this.owner = owner;
  }

  @Override
  public String toString() {
    return "PgLibrary{"
        + "name='"
        + name
        + '\''
        + ", language_oid="
        + language_oid
        + ", file_store_id="
        + file_store_id
        + ", owner="
        + owner
        + '}';
  }
}
