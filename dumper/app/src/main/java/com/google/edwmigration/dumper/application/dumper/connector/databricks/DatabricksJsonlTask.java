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
package com.google.edwmigration.dumper.application.dumper.connector.databricks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

class DatabricksJsonlTask extends AbstractTask<Void> {
  enum Output {
    CATALOGS,
    DATABASES,
    TABLES
  }

  private final ObjectMapper mapper = new ObjectMapper();
  private final Output output;

  DatabricksJsonlTask(String targetPath, Output output) {
    super(targetPath);
    this.output = output;
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksClient client = ((DatabricksHandle) handle).getClient();
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      if (output == Output.CATALOGS) {
        writeRecords(writer, client.listCatalogs());
      } else if (output == Output.DATABASES) {
        for (Map<String, Object> catalog : client.listCatalogs()) {
          String catalogName = value(catalog, "name");
          if (catalogName != null) writeRecords(writer, client.listSchemas(catalogName));
        }
      } else {
        writeTables(writer, client);
      }
    }
    return null;
  }

  private void writeTables(Writer writer, DatabricksClient client) throws Exception {
    for (Map<String, Object> catalog : client.listCatalogs()) {
      String catalogName = value(catalog, "name");
      if (catalogName == null) continue;
      for (Map<String, Object> schema : client.listSchemas(catalogName)) {
        String schemaName = value(schema, "name");
        if (schemaName == null) continue;
        for (Map<String, Object> table : client.listTables(catalogName, schemaName)) {
          Map<String, Object> record = new HashMap<>(table);
          record.putIfAbsent("catalog_name", catalogName);
          record.putIfAbsent("schema_name", schemaName);
          writeRecord(writer, record);
        }
      }
    }

    if (client.hasWarehouse()) {
      for (Map<String, Object> schema : client.listHiveMetastoreSchemas()) {
        String schemaName = value(schema, "databaseName", "database", "namespace");
        if (schemaName == null) continue;
        for (Map<String, Object> table : client.listHiveMetastoreTables(schemaName)) {
          String tableName = value(table, "tableName", "name");
          if (tableName == null) continue;
          Map<String, Object> record = new HashMap<>(table);
          record.put("catalog_name", "hive_metastore");
          record.put("schema_name", schemaName);
          record.put("table_name", tableName);
          record.put("columns", normalizeColumns(client.describeHiveMetastoreTable(schemaName, tableName)));
          writeRecord(writer, record);
        }
      }
    }
  }

  private void writeRecords(Writer writer, List<Map<String, Object>> records) throws Exception {
    for (Map<String, Object> record : records) writeRecord(writer, record);
  }

  private void writeRecord(Writer writer, Map<String, Object> record) throws Exception {
    writer.write(mapper.writeValueAsString(record));
    writer.write('\n');
  }

  private String value(Map<String, Object> values, String... keys) {
    for (String key : keys) {
      Object value = values.get(key);
      if (value != null) return String.valueOf(value);
    }
    return null;
  }

  private List<Map<String, Object>> normalizeColumns(List<Map<String, Object>> describedColumns) {
    List<Map<String, Object>> columns = new ArrayList<>();
    for (Map<String, Object> describedColumn : describedColumns) {
      String name = value(describedColumn, "col_name", "colName");
      String type = value(describedColumn, "data_type", "dataType");
      if (name == null || type == null || name.isEmpty() || name.startsWith("#")) continue;
      Map<String, Object> column = new HashMap<>();
      column.put("name", name);
      column.put("type_text", type);
      column.put("type_name", type.toUpperCase(java.util.Locale.ROOT));
      column.put("position", columns.size());
      String comment = value(describedColumn, "comment");
      if (comment != null) column.put("comment", comment);
      columns.add(column);
    }
    return columns;
  }
}