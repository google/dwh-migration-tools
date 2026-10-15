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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksCatalogsTaskTest {
  private final ByteArrayOutputStream output = new ByteArrayOutputStream();
  private final ByteSink sink =
      new ByteSink() {
        @Override
        public OutputStream openStream() {
          return output;
        }
      };

  @Before
  public void setUp() {
    output.reset();
  }

  @Test
  public void hiveMetastoreTable_isDumped() throws Exception {
    DatabricksClient client = new TestClient(true);
    new DatabricksCatalogsTask()
        .doRun(
            org.mockito.Mockito.mock(TaskRunContext.class),
            sink,
            new DatabricksHandle(client));

    String jsonl = output.toString(StandardCharsets.UTF_8.name());
    assertTrue(jsonl.contains("\"catalog\":\"hive_metastore\""));
    assertTrue(jsonl.contains("\"schema\":\"legacy_db\""));
    assertTrue(jsonl.contains("\"table\":\"legacy_table\""));
    assertTrue(jsonl.contains("\"table_full\":\"hive_metastore.legacy_db.legacy_table\""));
    assertTrue(jsonl.contains("\"columns\":[{"));
    assertTrue(jsonl.contains("\"name\":\"employee_id\""));
    assertTrue(jsonl.contains("\"type_text\":\"INT\""));
  }

  @Test
  public void unityCatalogOnly_isDumpedWithoutWarehouse() throws Exception {
    DatabricksClient client = new TestClient(false);

    new DatabricksCatalogsTask()
        .doRun(
            org.mockito.Mockito.mock(TaskRunContext.class),
            sink,
            new DatabricksHandle(client));

    String jsonl = output.toString(StandardCharsets.UTF_8.name());
    assertTrue(jsonl.contains("\"catalog\":\"unity_catalog\""));
    assertTrue(jsonl.contains("\"table_full\":\"unity_catalog.default.departments\""));
    assertFalse(jsonl.contains("\"catalog\":\"hive_metastore\""));
  }

  @Test
  public void unityCatalogAndHiveMetastore_areBothDumpedWithWarehouse() throws Exception {
    DatabricksClient client = new TestClient(true);

    new DatabricksCatalogsTask()
        .doRun(
            org.mockito.Mockito.mock(TaskRunContext.class),
            sink,
            new DatabricksHandle(client));

    String jsonl = output.toString(StandardCharsets.UTF_8.name());
    assertTrue(jsonl.contains("\"catalog\":\"unity_catalog\""));
    assertTrue(jsonl.contains("\"catalog\":\"hive_metastore\""));
    assertTrue(jsonl.contains("\"table_full\":\"unity_catalog.default.departments\""));
    assertTrue(jsonl.contains("\"table_full\":\"hive_metastore.legacy_db.legacy_table\""));
  }

  private static class TestClient extends DatabricksClient {
    private final boolean includeHiveMetastore;

    TestClient(boolean includeHiveMetastore) {
      super(
          "https://databricks.example", "token", includeHiveMetastore ? "warehouse-id" : null);
      this.includeHiveMetastore = includeHiveMetastore;
    }

    @Override
    public List<Map<String, Object>> listCatalogs() {
      return ImmutableList.of(ImmutableMap.of("name", "unity_catalog"));
    }

    @Override
    public List<Map<String, Object>> listSchemas(String catalogName) {
      return ImmutableList.of(ImmutableMap.of("name", "default"));
    }

    @Override
    public List<Map<String, Object>> listTables(String catalogName, String schemaName) {
      return ImmutableList.of(
          ImmutableMap.of("name", "departments", "full_name", "unity_catalog.default.departments"));
    }

    @Override
    public List<Map<String, Object>> listHiveMetastoreSchemas() {
      if (!includeHiveMetastore) return ImmutableList.of();
      return ImmutableList.of(ImmutableMap.of("databaseName", "legacy_db"));
    }

    @Override
    public List<Map<String, Object>> listHiveMetastoreTables(String schemaName) {
      if (!includeHiveMetastore) return ImmutableList.of();
      return ImmutableList.of(
          ImmutableMap.of(
              "database", schemaName, "tableName", "legacy_table", "isTemporary", "false"));
    }

    @Override
    public List<Map<String, Object>> describeHiveMetastoreTable(
        String schemaName, String tableName) {
      if (!includeHiveMetastore) return ImmutableList.of();
      return ImmutableList.of(
          ImmutableMap.of("col_name", "employee_id", "data_type", "INT"));
    }
  }
}