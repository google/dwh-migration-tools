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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksJsonlTaskTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void catalogs_areWrittenAsJsonlRecords() throws Exception {
    TestClient client = new TestClient(false);

    JsonNode records = run(DatabricksJsonlTask.Output.CATALOGS, client);

    assertEquals(1, records.size());
    assertEquals("unity_catalog", records.get(0).get("name").asText());
  }

  @Test
  public void databases_skipCatalogsWithoutNames() throws Exception {
    TestClient client = new TestClient(false);
    client.catalogs.add(new HashMap<String, Object>());

    JsonNode records = run(DatabricksJsonlTask.Output.DATABASES, client);

    assertEquals(1, records.size());
    assertEquals("default", records.get(0).get("name").asText());
  }

  @Test
  public void tables_includeUnityAndHiveMetadata() throws Exception {
    TestClient client = new TestClient(true);

    JsonNode records = run(DatabricksJsonlTask.Output.TABLES, client);

    assertEquals(2, records.size());
    JsonNode unityTable = records.get(0);
    assertEquals("preserved", unityTable.get("catalog_name").asText());
    assertEquals("default", unityTable.get("schema_name").asText());

    JsonNode hiveTable = records.get(1);
    assertEquals("hive_metastore", hiveTable.get("catalog_name").asText());
    assertEquals("legacy_db", hiveTable.get("schema_name").asText());
    assertEquals("legacy_table", hiveTable.get("table_name").asText());
    assertEquals(1, hiveTable.get("columns").size());
    assertEquals("employee_id", hiveTable.get("columns").get(0).get("name").asText());
    assertEquals("BIGINT", hiveTable.get("columns").get(0).get("type_name").asText());
    assertEquals(0, hiveTable.get("columns").get(0).get("position").asInt());
    assertEquals("Employee identifier", hiveTable.get("columns").get(0).get("comment").asText());
    assertFalse(hiveTable.get("columns").toString().contains("# col_name"));
    assertTrue(hiveTable.get("columns").get(0).has("comment"));
  }

  private JsonNode run(DatabricksJsonlTask.Output output, TestClient client) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ByteSink sink =
        new ByteSink() {
          @Override
          public OutputStream openStream() {
            return bytes;
          }
        };

    new DatabricksJsonlTask("output.jsonl", output)
        .doRun(
            org.mockito.Mockito.mock(TaskRunContext.class), sink, new DatabricksHandle(client));

    List<JsonNode> records = new ArrayList<>();
    for (String line : bytes.toString(StandardCharsets.UTF_8.name()).split("\\n")) {
      if (!line.isEmpty()) records.add(mapper.readTree(line));
    }
    return mapper.valueToTree(records);
  }

  private static class TestClient extends DatabricksClient {
    private final boolean includeHiveMetastore;
    private final List<Map<String, Object>> catalogs = new ArrayList<>();

    TestClient(boolean includeHiveMetastore) {
      super(
          "https://databricks.example/", "token", includeHiveMetastore ? "warehouse-id" : null);
      this.includeHiveMetastore = includeHiveMetastore;
      catalogs.add(ImmutableMap.of("name", "unity_catalog"));
    }

    @Override
    public List<Map<String, Object>> listCatalogs() {
      return catalogs;
    }

    @Override
    public List<Map<String, Object>> listSchemas(String catalogName) {
      return ImmutableList.of(ImmutableMap.of("name", "default"));
    }

    @Override
    public List<Map<String, Object>> listTables(String catalogName, String schemaName) {
      Map<String, Object> table = new HashMap<>();
      table.put("name", "unity_table");
      table.put("catalog_name", "preserved");
      return ImmutableList.of(table);
    }

    @Override
    public boolean hasWarehouse() {
      return includeHiveMetastore;
    }

    @Override
    public List<Map<String, Object>> listHiveMetastoreSchemas() {
      return ImmutableList.of(ImmutableMap.of("databaseName", "legacy_db"));
    }

    @Override
    public List<Map<String, Object>> listHiveMetastoreTables(String schemaName) {
      return ImmutableList.of(ImmutableMap.of("tableName", "legacy_table"));
    }

    @Override
    public List<Map<String, Object>> describeHiveMetastoreTable(
        String schemaName, String tableName) {
      return ImmutableList.of(
          ImmutableMap.of("col_name", "# col_name", "data_type", "STRING"),
          ImmutableMap.of(
              "colName", "employee_id",
              "dataType", "bigint",
              "comment", "Employee identifier"),
          ImmutableMap.of("col_name", "missing_type"));
    }
  }
}
