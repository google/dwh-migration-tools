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

public class DatabricksCatalogsTask extends AbstractTask<Void> {
  private final ObjectMapper mapper = new ObjectMapper();

  public DatabricksCatalogsTask() {
    super("databricks-uc.jsonl");
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle dbHandle = (DatabricksHandle) handle;
    DatabricksClient client = dbHandle.getClient();

    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      List<Map<String, Object>> catalogs = client.listCatalogs();
      for (Map<String, Object> catalog : catalogs) {
        String catalogName = value(catalog, "name");
        if (catalogName == null) continue;
        List<Map<String, Object>> schemas = client.listSchemas(catalogName);
        for (Map<String, Object> schema : schemas) {
          String schemaName = value(schema, "name");
          if (schemaName == null) continue;
          List<Map<String, Object>> tables = client.listTables(catalogName, schemaName);
          for (Map<String, Object> table : tables) {
            writeTable(writer, client, catalogName, schemaName, value(table, "name"), table);
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
            Map<String, Object> metadata = new HashMap<>(table);
            metadata.put(
                "columns", normalizeColumns(client.describeHiveMetastoreTable(schemaName, tableName)));
            writeTable(writer, client, "hive_metastore", schemaName, tableName, metadata);
          }
        }
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return "Dump Databricks Unity Catalog and Hive Metastore tables to databricks-uc.jsonl";
  }

  private void writeTable(
      Writer writer,
      DatabricksClient client,
      String catalogName,
      String schemaName,
      String tableName,
      Map<String, Object> table)
      throws Exception {
    Map<String, Object> out = Map.of(
        "workspace", client.getBaseUrl(),
        "catalog", catalogName,
        "schema", schemaName,
        "table", tableName,
      "table_full", fullName(table, catalogName, schemaName, tableName),
        "meta", table);
    writer.write(mapper.writeValueAsString(out));
    writer.write("\n");
  }

  private String value(Map<String, Object> values, String... keys) {
    for (String key : keys) {
      Object value = values.get(key);
      if (value != null) return String.valueOf(value);
    }
    return null;
  }

  private String fullName(
      Map<String, Object> table, String catalogName, String schemaName, String tableName) {
    String fullName = value(table, "full_name");
    return fullName == null ? catalogName + "." + schemaName + "." + tableName : fullName;
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
