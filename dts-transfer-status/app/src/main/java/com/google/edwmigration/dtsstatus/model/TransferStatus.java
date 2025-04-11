/*
 * Copyright 2022-2025 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.edwmigration.dtsstatus.model;

import com.google.protobuf.Timestamp;
import org.checkerframework.checker.nullness.qual.NonNull;

public class TransferStatus implements Comparable<TransferStatus> {
  private final String database;
  private final String table;
  private final String status;
  private final Timestamp timestamp;

  public TransferStatus(String database, String table, String status, Timestamp timestamp) {
    this.database = database;
    this.table = table;
    this.status = status;
    this.timestamp = timestamp;
  }

  public String getDatabase() {
    return database;
  }

  public String getStatus() {
    return status;
  }

  public String getTable() {
    return table;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return database + "\t" + table + "\t" + status;
  }

  @Override
  public int compareTo(@NonNull TransferStatus other) {
    int comparisonResult = this.getDatabase().compareTo(other.getDatabase());
    if (comparisonResult != 0) {
      return comparisonResult;
    }
    comparisonResult = this.getTable().compareTo(other.getTable());
    if (comparisonResult != 0) {
      return comparisonResult;
    }
    return compareTimestamp(this.getTimestamp(), other.getTimestamp());
  }

  private static int compareTimestamp(final Timestamp t1, final Timestamp t2) {
    if (t1.getSeconds() > t2.getSeconds()) {
      return 1;
    }
    if (t1.getSeconds() < t2.getSeconds()) {
      return -1;
    }
    return Integer.compare(t1.getNanos(), t2.getNanos());
  }
}
