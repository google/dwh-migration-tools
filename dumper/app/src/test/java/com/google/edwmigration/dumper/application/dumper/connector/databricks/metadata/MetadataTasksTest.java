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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static com.google.edwmigration.dumper.application.dumper.task.AbstractTask.FORMAT;
import static com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ERROR_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.DatabricksFilter;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.UnityCatalogTasks.TableName;
import com.google.edwmigration.dumper.application.dumper.io.FileSystemOutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import com.google.edwmigration.dumper.application.dumper.task.TaskState;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.CatalogsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ColumnMasksFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ColumnsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.RowFiltersFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.SchemataFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.TableConstraintsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.TablesFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ViewsFormat;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the Unity Catalog and the hive_metastore tasks with the recorded responses, and the task of
 * the connector that fails the run when every metadata task fails.
 */
@RunWith(JUnit4.class)
public class MetadataTasksTest {

  private static final String TABLES_PATH = "/api/2.1/unity-catalog/tables";
  private static final String STATEMENTS_PATH = "/api/2.0/sql/statements";
  private static final String DESCRIBE_TABLE_STATEMENT = "DESCRIBE TABLE EXTENDED .* AS JSON";

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DatabricksFilter ACCEPT_ALL =
      new DatabricksFilter(ImmutableList.of(), ImmutableList.of());
  private static final ConnectorArguments ARGUMENTS =
      ConnectorArguments.create(
          ImmutableList.of("--connector", "databricks", "--warehouse", "wh-test"));

  private static WireMockServer server;

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void startServer() {
    server = new WireMockServer(wireMockConfig().dynamicPort());
    server.start();
  }

  @AfterClass
  public static void stopServer() {
    server.stop();
  }

  /**
   * Stubs the recorded Unity Catalog REST responses ({@code unity-catalog-responses.json}, keyed by
   * GET path and query) and the recorded {@code hive_metastore} SQL statements ({@code
   * hive-metastore-responses.json}, keyed by SQL statement and mapped to {@code rows} or {@code
   * json_cell}). A test replaces a recorded response by adding its own stub, because WireMock uses
   * the most recently added stub that matches a request.
   */
  @Before
  public void stubRecordedResponses() throws IOException {
    server.resetAll();
    for (Map.Entry<String, JsonNode> entry : loadTestCases("unity-catalog-responses.json")) {
      server.stubFor(
          get(urlEqualTo(entry.getKey())).willReturn(okJson(entry.getValue().toString())));
    }
    int index = 0;
    for (Map.Entry<String, JsonNode> entry : loadTestCases("hive-metastore-responses.json")) {
      JsonNode spec = entry.getValue();
      String rows =
          spec.has("json_cell")
              ? MAPPER.writeValueAsString(
                  ImmutableList.of(ImmutableList.of(spec.get("json_cell").toString())))
              : spec.path("rows").toString();
      stubStatement(entry.getKey(), "/chunk/" + index++, rows, spec.get("columns"));
    }
  }

  @Test
  public void tasks_writeExpectedCsvsFromRecordedFixtures() throws Exception {
    Path directory = temporaryFolder.newFolder().toPath();
    ImmutableList<Task<?>> tasks = allTasks();
    runTasks(tasks, ACCEPT_ALL, directory);

    // Each written file matches its file in expected/.
    for (String entryName : listFiles(directory)) {
      assertEquals(
          entryName,
          readExpectedFile(entryName),
          readFile(directory, entryName).replace("\r\n", "\n"));
    }
    // No object was skipped, so no task summary has a warning.
    for (Task<?> task : tasks) {
      assertFalse(task.toString(), task.toString().contains("WARNING"));
    }
    // Views have no constraints, so they're never fetched with tables.get.
    server.verify(
        0,
        getRequestedFor(
            urlPathEqualTo(TABLES_PATH + "/dumper_test_uc.dumper_test_schema.items_view")));
  }

  @Test
  public void catalogsAndSchemataTasks_applyCustomFilters() throws Exception {
    DatabricksFilter customFilter =
        new DatabricksFilter(
            ImmutableList.of("DUMPER_TEST_UC", "System", HIVE_METASTORE_CATALOG),
            ImmutableList.of("Dumper_Test_Schema", "information_schema"));
    // The listed system catalog is accepted. Its schemas.list isn't recorded, so it fails and is
    // skipped.
    assertEquals(
        "CatalogName,CatalogOwner\ndumper_test_uc,owner@example.com\nsystem,System user\n",
        runTask(CatalogsFormat.ZIP_ENTRY_NAME, customFilter));
    assertEquals(
        "CatalogName,SchemaName\ndumper_test_uc,dumper_test_schema\n",
        runTask(SchemataFormat.ZIP_ENTRY_NAME, customFilter));
  }

  @Test
  public void tasks_skipFailedObjectsAndFailWhenEveryObjectFails() throws Exception {
    // 1. A failed object is skipped, and the summary of its task counts it.
    server.stubFor(
        get(urlPathEqualTo(TABLES_PATH + "/dumper_test_uc.dumper_test_schema.orgs"))
            .willReturn(permissionDenied("User does not have SELECT.")));
    stubFailed("DESCRIBE TABLE EXTENDED `dumper_test_hms`.`parquet_items` AS JSON");
    // DESCRIBE DETAIL fails with an API error instead of a failed statement.
    server.stubFor(
        post(urlEqualTo(STATEMENTS_PATH))
            .withRequestBody(
                matchingJsonPath(
                    "$.statement", equalTo("DESCRIBE DETAIL `dumper_test_hms`.`delta_items`")))
            .willReturn(permissionDenied("No access to the SQL warehouse.")));
    Path directory = temporaryFolder.newFolder().toPath();
    ImmutableList<Task<?>> tasks = allTasks();
    runTasks(tasks, ACCEPT_ALL, directory);
    String summaries = tasks.toString();
    assertTrue(summaries, summaries.contains("(WARNING: tables.get failed for 1 of 2 objects)"));
    assertTrue(
        summaries,
        summaries.contains("(WARNING: DESCRIBE TABLE EXTENDED AS JSON failed for 1 of 3 objects)"));
    // orgs gets an ERROR row, and items keeps its constraints.
    String constraintsCsv = readFile(directory, TableConstraintsFormat.ZIP_ENTRY_NAME);
    assertTrue(constraintsCsv.contains("items_pk"));
    CSVRecord orgs = Iterables.getOnlyElement(findTableRecords(constraintsCsv, "orgs"));
    assertEquals(ERROR_TYPE, orgs.get(TableConstraintsFormat.Header.ConstraintType.name()));
    // parquet_items gets an ERROR row and no columns.
    String tablesCsv = readFile(directory, TablesFormat.HMS_ZIP_ENTRY_NAME);
    String columnsCsv = readFile(directory, ColumnsFormat.HMS_ZIP_ENTRY_NAME);
    CSVRecord parquetItems = Iterables.getOnlyElement(findTableRecords(tablesCsv, "parquet_items"));
    assertEquals(ERROR_TYPE, parquetItems.get(TablesFormat.Header.TableType.name()));
    assertFalse(columnsCsv.contains("parquet_items"));
    // Without DESCRIBE DETAIL, delta_items has no size, and its dt column no partition index.
    CSVRecord deltaItems = Iterables.getOnlyElement(findTableRecords(tablesCsv, "delta_items"));
    assertEquals("", deltaItems.get(TablesFormat.Header.SizeInBytes.name()));
    CSVRecord dt =
        Iterables.find(
            findTableRecords(columnsCsv, "delta_items"),
            column -> "dt".equals(column.get(ColumnsFormat.Header.ColumnName.name())));
    assertEquals("", dt.get(ColumnsFormat.Header.PartitionIndex.name()));

    // 2. When every tables.get fails, the task fails.
    server.stubFor(
        get(urlPathMatching(TABLES_PATH + "/.+")).willReturn(permissionDenied("No SELECT.")));
    assertTrue(
        assertThrows(
                IOException.class, () -> runTask(TableConstraintsFormat.ZIP_ENTRY_NAME, ACCEPT_ALL))
            .getMessage()
            .contains("tables.get failed for all"));

    // 3. When every tables.list fails, the five table files fail together: none is committed, and
    // only their temporary files exist.
    server.stubFor(get(urlPathEqualTo(TABLES_PATH)).willReturn(permissionDenied("No USE SCHEMA.")));
    Task<?> tablesTask = findTask(TablesFormat.ZIP_ENTRY_NAME);
    Path failedDirectory = temporaryFolder.newFolder().toPath();
    assertThrows(
        IOException.class,
        () -> runTasks(ImmutableList.of(tablesTask), ACCEPT_ALL, failedDirectory));
    assertEquals(
        ImmutableSortedSet.of(
            TablesFormat.ZIP_ENTRY_NAME + ".tmp",
            ColumnsFormat.ZIP_ENTRY_NAME + ".tmp",
            ViewsFormat.ZIP_ENTRY_NAME + ".tmp",
            ColumnMasksFormat.ZIP_ENTRY_NAME + ".tmp",
            RowFiltersFormat.ZIP_ENTRY_NAME + ".tmp"),
        listFiles(failedDirectory));
  }

  @Test
  public void hiveMetastoreTableTask_replacesFilesCommittedByEarlierRun() throws Exception {
    // An earlier run committed columns-hms.csv but not tables-hms.csv, the only file that
    // --continue checks, so the rerun of the task writes columns-hms.csv again.
    Path directory = temporaryFolder.newFolder().toPath();
    Files.write(directory.resolve(ColumnsFormat.HMS_ZIP_ENTRY_NAME), "stale".getBytes(UTF_8));

    runTasks(ImmutableList.of(findTask(TablesFormat.HMS_ZIP_ENTRY_NAME)), ACCEPT_ALL, directory);

    assertEquals(
        readExpectedFile(ColumnsFormat.HMS_ZIP_ENTRY_NAME),
        readFile(directory, ColumnsFormat.HMS_ZIP_ENTRY_NAME));
  }

  @Test
  public void hiveMetastoreTableTask_failsOnFirstTableWhenWarehouseCannotParseAsJson()
      throws Exception {
    // A SQL warehouse before Databricks Runtime 16.2 can't parse DESCRIBE TABLE ... AS JSON.
    server.stubFor(
        post(urlEqualTo(STATEMENTS_PATH))
            .withRequestBody(matchingJsonPath("$.statement", matching(DESCRIBE_TABLE_STATEMENT)))
            .willReturn(
                failedStatement(
                    "[PARSE_SYNTAX_ERROR] Syntax error at or near 'JSON': extra input 'JSON'.")));

    IOException thrown =
        assertThrows(IOException.class, () -> runTask(TablesFormat.HMS_ZIP_ENTRY_NAME, ACCEPT_ALL));

    assertTrue(thrown.getMessage(), thrown.getMessage().contains("Databricks Runtime 16.2"));
    // The other tables aren't described, because their statements would fail the same way.
    server.verify(
        1,
        postRequestedFor(urlEqualTo(STATEMENTS_PATH))
            .withRequestBody(matchingJsonPath("$.statement", matching(DESCRIBE_TABLE_STATEMENT))));
  }

  @Test
  public void getTableJson_escapesFullNameAsPathSegment() throws Exception {
    String path = TABLES_PATH + "/main.sales.my%20orders%231%3F%25";
    server.stubFor(get(urlPathEqualTo(path)).willReturn(okJson("{\"name\":\"my orders#1?%\"}")));
    try (DatabricksHandle handle = newHandle(ACCEPT_ALL)) {
      assertEquals(
          "my orders#1?%",
          UnityCatalogTasks.getTableJson(handle, new TableName("main", "sales", "my orders#1?%"))
              .path("name")
              .asText());
    }
  }

  @Test
  public void allFailedCheckTask_failsOnlyWhenEveryMetadataTaskFails() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();
    new DatabricksMetadataConnector().addTasksTo(tasks, ARGUMENTS);
    Task<?> checkTask = Iterables.getLast(tasks);
    Set<String> succeededPaths = new HashSet<>();
    TaskSetState state = mock(TaskSetState.class);
    when(state.getTaskState(any()))
        .thenAnswer(
            invocation -> {
              Task<?> task = invocation.getArgument(0);
              return succeededPaths.contains(task.getTargetPath())
                  ? TaskState.SUCCEEDED
                  : TaskState.FAILED;
            });
    assertTrue(checkTask.getConditions()[0].evaluate(state));
    // catalogs-hms.csv is written without a SQL statement, so its success alone doesn't count.
    succeededPaths.add(CatalogsFormat.HMS_ZIP_ENTRY_NAME);
    assertTrue(checkTask.getConditions()[0].evaluate(state));
    succeededPaths.add(CatalogsFormat.ZIP_ENTRY_NAME);
    assertFalse(checkTask.getConditions()[0].evaluate(state));

    IOException thrown =
        assertThrows(IOException.class, () -> checkTask.run(mock(TaskRunContext.class)));
    assertTrue(
        thrown.getMessage(), thrown.getMessage().contains("All Databricks metadata tasks failed."));
  }

  /** Runs {@code tasks} with one handle, with their files in {@code directory}. */
  private static void runTasks(
      Iterable<? extends Task<?>> tasks, DatabricksFilter filter, Path directory) throws Exception {
    try (DatabricksHandle handle = newHandle(filter)) {
      TaskRunContext context =
          new TaskRunContext(
              new FileSystemOutputHandleFactory(directory), handle, 1, null, ARGUMENTS);
      for (Task<?> task : tasks) {
        task.run(context);
      }
    }
  }

  /** Runs the task of {@code targetPath} as the only task of a new run, and returns its file. */
  private String runTask(String targetPath, DatabricksFilter filter) throws Exception {
    Path directory = temporaryFolder.newFolder().toPath();
    runTasks(ImmutableList.of(findTask(targetPath)), filter, directory);
    return readFile(directory, targetPath);
  }

  /** Returns new Unity Catalog and hive_metastore tasks. */
  private static ImmutableList<Task<?>> allTasks() {
    return ImmutableList.copyOf(
        Iterables.concat(UnityCatalogTasks.create(), HiveMetastoreTasks.create()));
  }

  /** Returns the task of {@code targetPath} from new Unity Catalog and hive_metastore tasks. */
  private static Task<?> findTask(String targetPath) {
    for (Task<?> task : allTasks()) {
      if (targetPath.equals(task.getTargetPath())) {
        return task;
      }
    }
    throw new AssertionError("No task for " + targetPath);
  }

  private static ImmutableSortedSet<String> listFiles(Path directory) {
    return ImmutableSortedSet.copyOf(directory.toFile().list());
  }

  private static String readFile(Path directory, String entryName) throws IOException {
    return new String(Files.readAllBytes(directory.resolve(entryName)), UTF_8);
  }

  /**
   * Returns the file of {@code entryName} in expected/, with LF line endings, because Git may check
   * out the expected files with CRLF line endings on Windows.
   */
  private static String readExpectedFile(String entryName) throws IOException {
    return Resources.toString(
            Resources.getResource("connector/databricks/expected/" + entryName), UTF_8)
        .replace("\r\n", "\n");
  }

  /**
   * Returns the records of a tables, columns or table_constraints CSV file whose {@code TableName}
   * is {@code tableName}, so that the tests read the values by column name.
   */
  private static ImmutableList<CSVRecord> findTableRecords(String csv, String tableName)
      throws IOException {
    ImmutableList.Builder<CSVRecord> records = ImmutableList.builder();
    try (CSVParser parser = CSVParser.parse(csv, FORMAT.withFirstRecordAsHeader())) {
      for (CSVRecord record : parser) {
        if (tableName.equals(record.get("TableName"))) {
          records.add(record);
        }
      }
    }
    return records.build();
  }

  private static DatabricksHandle newHandle(DatabricksFilter filter) {
    WorkspaceClient client =
        new WorkspaceClient(
            new DatabricksConfig()
                .setHost(server.baseUrl())
                .setToken("test-token")
                .setAuthType("pat"));
    return new DatabricksHandle(new DatabricksSqlHelper(client, "wh-test"), client, filter);
  }

  private static Set<Map.Entry<String, JsonNode>> loadTestCases(String name) throws IOException {
    return MAPPER
        .readTree(Resources.toString(Resources.getResource("connector/databricks/" + name), UTF_8))
        .properties();
  }

  private static ResponseDefinitionBuilder permissionDenied(String message) {
    return aResponse()
        .withStatus(403)
        .withHeader("Content-Type", "application/json")
        .withBody(
            String.format("{\"error_code\": \"PERMISSION_DENIED\", \"message\": \"%s\"}", message));
  }

  /**
   * Stubs a statement that succeeds with one chunk of {@code rows}, a JSON array of rows. {@code
   * columns} is the JSON array of the result columns, or null.
   */
  private static void stubStatement(String sql, String chunkPath, String rows, JsonNode columns) {
    server.stubFor(get(urlEqualTo(chunkPath)).willReturn(okJson(rows)));
    server.stubFor(
        post(urlEqualTo(STATEMENTS_PATH))
            .withRequestBody(matchingJsonPath("$.statement", equalTo(sql)))
            .willReturn(
                okJson(
                    String.format(
                        "{\"statement_id\": \"stmt-ok\", \"status\": {\"state\": \"SUCCEEDED\"},"
                            + " \"manifest\": {\"total_chunk_count\": 1, \"schema\": {\"columns\":"
                            + " %s}}, \"result\": {\"external_links\": [{\"external_link\":"
                            + " \"%s%s\"}]}}",
                        columns, server.baseUrl(), chunkPath))));
  }

  private static void stubFailed(String sql) {
    server.stubFor(
        post(urlEqualTo(STATEMENTS_PATH))
            .withRequestBody(matchingJsonPath("$.statement", equalTo(sql)))
            .willReturn(failedStatement("[INSUFFICIENT_PERMISSIONS] No access.")));
  }

  /** Returns the response of a statement that failed with the error {@code message}. */
  private static ResponseDefinitionBuilder failedStatement(String message) {
    return okJson(
        String.format(
            "{\"statement_id\": \"stmt-failed\", \"status\": {\"state\": \"FAILED\","
                + " \"error\": {\"message\": \"%s\"}}}",
            message));
  }
}
