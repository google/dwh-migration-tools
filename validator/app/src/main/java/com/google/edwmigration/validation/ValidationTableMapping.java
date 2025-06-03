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
package com.google.edwmigration.validation;

import javax.annotation.Nullable;

/** @author nehanene */
public class ValidationTableMapping {
  public enum TableType {
    SOURCE,
    TARGET
  }

  public static class ValidationTable {
    private String schema = null;
    private final String table;
    private final String fullyQualifiedTable;
    private final TableType tableType;

    public ValidationTable(
        String fullyQualifiedTable, String schema, String table, TableType tableType) {
      this.schema = schema;
      this.table = table;
      this.fullyQualifiedTable = fullyQualifiedTable;
      this.tableType = tableType;
    }

    public ValidationTable(String fullyQualifiedTable, String table, TableType tableType) {
      this.table = table;
      this.fullyQualifiedTable = fullyQualifiedTable;
      this.tableType = tableType;
    }

    @Nullable
    public String getSchema() {
      return schema;
    }

    public String getTable() {
      return table;
    }

    public TableType getTableType() {
      return tableType;
    }

    public String getFullyQualifiedTable() {
      return fullyQualifiedTable;
    }

    public String getDefaultSchema(String defaultSchema) {
      if (schema == null) {
        return defaultSchema;
      }
      return schema;
    }
  }

  private final ValidationTable sourceTable;
  private final ValidationTable targetTable;

  public ValidationTableMapping(String source, String target) {

    String[] sourceTableName = source.split("\\.");
    String[] targetTableName = target.split("\\.");
    if (sourceTableName.length == 2) {
      sourceTable =
          new ValidationTable(source, sourceTableName[0], sourceTableName[1], TableType.SOURCE);
    } else if (sourceTableName.length == 1) {
      sourceTable = new ValidationTable(source, sourceTableName[0], TableType.SOURCE);
    } else {
      throw new IllegalArgumentException("Invalid source table name provided: " + source);
    }

    if (targetTableName.length == 2) {
      targetTable =
          new ValidationTable(target, targetTableName[0], targetTableName[1], TableType.TARGET);
    } else if (targetTableName.length == 1) {
      targetTable = new ValidationTable(target, targetTableName[0], TableType.TARGET);
    } else {
      throw new IllegalArgumentException("Invalid target table name provided: " + target);
    }
  }

  public ValidationTable getSourceTable() {
    return sourceTable;
  }

  public ValidationTable getTargetTable() {
    return targetTable;
  }
}
