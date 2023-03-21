/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.hive.support;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;

public class HiveTestSchemaBuilder {

  public static final String DATABASE_PREFIX = "test_db_";
  public static final String TABLE_PREFIX = "test_tbl_";
  public static final List<String> PARTITION_NAMES = Lists.newArrayList("test_partition");
  public static final int PARTITION_SPLIT = 50;

  public static List<List<String>> getStatements(
      int dbCount, int tblCount, int colCount, int pCount) {
    List<List<String>> result = new ArrayList<>();
    result.add(createDatabases(dbCount));
    result.add(createTables(dbCount, tblCount, colCount));
    result.add(createPartitions(dbCount, tblCount, pCount));
    return result;
  }

  public static List<String> createPartitions(int dbCount, int tblCount, int pCount) {
    List<String> result = new ArrayList<>();
    for (int db = 0; db < dbCount; db++) {
      for (int tbl = 0; tbl < tblCount; tbl++) {
        result.addAll(createPartitions(db, tbl, pCount, PARTITION_SPLIT));
      }
    }
    return result;
  }

  public static List<String> createTables(int dbCount, int tblCount, int colCount) {
    List<String> result = new ArrayList<>();
    for (int db = 0; db < dbCount; db++) {
      for (int tbl = 0; tbl < tblCount; tbl++) {
        result.add(createTable(db, tbl, colCount));
      }
    }
    return result;
  }

  public static List<String> createDatabases(int dbCount) {
    return IntStream.range(0, dbCount)
        .mapToObj(HiveTestSchemaBuilder::createDatabase)
        .collect(Collectors.toList());
  }

  public static List<String> createPartitions(
      int dbIndex, int tblIndex, int partitionCount, int partitionSplit) {
    List<String> partitions =
        IntStream.range(0, partitionCount)
            .mapToObj(HiveTestSchemaBuilder::getPartitionValue)
            .collect(Collectors.toList());

    return Lists.partition(partitions, partitionSplit).stream()
        .map(l -> StringUtils.join(l, " "))
        .map(
            p ->
                String.format(
                    "alter table %s%d.%s%d add %s",
                    DATABASE_PREFIX, dbIndex, TABLE_PREFIX, tblIndex, p))
        .collect(Collectors.toList());
  }

  public static String createTable(int dbIndex, int tblIndex, int colCount) {
    return String.format(
        "create table %s%d.%s%d (%s) partitioned by (%s)",
        DATABASE_PREFIX,
        dbIndex,
        TABLE_PREFIX,
        tblIndex,
        getColumns(colCount),
        getPartitionsDefinition());
  }

  private static String getColumns(int colCount) {
    return IntStream.range(0, colCount)
        .mapToObj(c -> "col_" + c + " int")
        .collect(Collectors.joining(", "));
  }

  public static String createDatabase(int dbIndex) {
    return String.format("create database %s%d", DATABASE_PREFIX, dbIndex);
  }

  private static String getPartitionsDefinition() {
    return PARTITION_NAMES.stream().map(p -> p + " int").collect(Collectors.joining(", "));
  }

  private static String getPartitionValue(int value) {
    return PARTITION_NAMES.stream()
        .map(p -> String.format("partition (%s = %s)", p, value))
        .collect(Collectors.joining(", "));
  }
}
