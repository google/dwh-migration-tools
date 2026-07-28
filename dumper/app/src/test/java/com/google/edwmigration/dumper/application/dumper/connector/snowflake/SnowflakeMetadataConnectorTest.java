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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
@RunWith(Theories.class)
public class SnowflakeMetadataConnectorTest extends AbstractSnowflakeConnectorExecutionTest {

  @SuppressWarnings("UnusedVariable")
  private static final Logger logger =
      LoggerFactory.getLogger(SnowflakeMetadataConnectorTest.class);

  private final MetadataConnector connector = new SnowflakeMetadataConnector();

  @Test
  public void testConnector() throws Exception {
    testConnectorDefaults(connector);
  }

  @Nonnull
  private static String iffaulty(int i, String s0, String s1) {
    return i == 0 ? s0 : s1;
  }

  @Test
  public void testExecution() throws Exception {
    for (int i = 0; i < 2; i++) {
      File outputFile =
          TestUtils.newOutputFile(
              "compilerworks-snowflake-metadata-auto-" + iffaulty(i, "is", "au") + ".zip");
      logger.debug("Output file: {}", outputFile.getAbsolutePath());

      if (!run(ARGS(connector, outputFile, "--test-flags", iffaulty(i, "", "A")))) continue;

      ZipValidator validator =
          new ZipValidator().withFormat(SnowflakeMetadataDumpFormat.FORMAT_NAME);

      validator.withEntryValidator(
          iffaulty(
              i,
              SnowflakeMetadataDumpFormat.DatabasesFormat.IS_ZIP_ENTRY_NAME,
              SnowflakeMetadataDumpFormat.DatabasesFormat.AU_ZIP_ENTRY_NAME),
          SnowflakeMetadataDumpFormat.DatabasesFormat.Header.class);
      validator.withEntryValidator(
          iffaulty(
              i,
              SnowflakeMetadataDumpFormat.SchemataFormat.IS_ZIP_ENTRY_NAME,
              SnowflakeMetadataDumpFormat.SchemataFormat.AU_ZIP_ENTRY_NAME),
          SnowflakeMetadataDumpFormat.SchemataFormat.Header.class);
      validator.withEntryValidator(
          iffaulty(
              i,
              SnowflakeMetadataDumpFormat.TablesFormat.IS_ZIP_ENTRY_NAME,
              SnowflakeMetadataDumpFormat.TablesFormat.AU_ZIP_ENTRY_NAME),
          SnowflakeMetadataDumpFormat.TablesFormat.Header.class);
      validator.withEntryValidator(
          iffaulty(
              i,
              SnowflakeMetadataDumpFormat.ColumnsFormat.IS_ZIP_ENTRY_NAME,
              SnowflakeMetadataDumpFormat.ColumnsFormat.AU_ZIP_ENTRY_NAME),
          SnowflakeMetadataDumpFormat.ColumnsFormat.Header.class);
      validator.withEntryValidator(
          iffaulty(
              i,
              SnowflakeMetadataDumpFormat.ViewsFormat.IS_ZIP_ENTRY_NAME,
              SnowflakeMetadataDumpFormat.ViewsFormat.AU_ZIP_ENTRY_NAME),
          SnowflakeMetadataDumpFormat.ViewsFormat.Header.class);
      validator.withEntryValidator(
          iffaulty(
              i,
              SnowflakeMetadataDumpFormat.FunctionsFormat.IS_ZIP_ENTRY_NAME,
              SnowflakeMetadataDumpFormat.FunctionsFormat.AU_ZIP_ENTRY_NAME),
          SnowflakeMetadataDumpFormat.FunctionsFormat.Header.class);
    }
  }

  // ./gradlew :compilerworks-application-dumper:{cleanTest,test} --tests
  // SnowflakeMetadataConnectorTest.testDatabaseNameFailure -Dtest-sys-prop.test.dumper=true
  // -Dtest.verbose=true
  @Test
  public void testDatabaseNameFailure() {
    Assume.assumeTrue(isDumperTest());

    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> {
              File outputFile =
                  TestUtils.newOutputFile("compilerworks-snowflake-metadata-fail.zip");
              String[] args = ARGS(connector, outputFile);

              assertEquals("--database", args[6]);
              args[7] = args[7] + "_NOT_EXISTS";
              run(args);
            });

    assertTrue(exception.getMessage().startsWith("Database name not found"));
  }

  @Test
  public void connector_noAssessment_doesNotContainFeatures() throws IOException {

    ImmutableMap<String, String> sqls = collectSqlStatements();

    assertFalse(sqls.containsKey("features.csv"));
  }

  @Test
  public void connector_noAssessment_generatesExpectedSql() throws IOException {
    TypeReference<Map<String, String>> typeReference = new TypeReference<Map<String, String>>() {};
    Map<String, String> expectedSqls =
        CoreMetadataDumpFormat.MAPPER.readValue(
            Resources.toString(
                Resources.getResource("connector/snowflake/jdbc-tasks-sql.yaml"),
                StandardCharsets.UTF_8),
            typeReference);

    ImmutableMap<String, String> sqls = collectSqlStatements();

    for (Entry<String, String> item : expectedSqls.entrySet()) {
      String key = item.getKey();
      assertTrue(key, sqls.containsKey(key));
      assertEquals(key, sqls.get(key), item.getValue());
    }
  }

  @Test
  public void connector_withAssessment_containsFeatures() throws IOException {

    ImmutableMap<String, String> sqls = collectSqlStatements("--assessment");

    assertTrue(sqls.containsKey("features.csv"));
  }

  @Theory
  public void featuresQueryPathValue_refersToExistingPath(FeaturesQueryPath path) {
    path.loadFile();
  }

  @Test
  public void connector_generatesExpectedSql_withQueryOverrides() throws IOException {
    Map<String, String> actualSqls =
        collectSqlStatements("-Dsnowflake.metadata.columns.query=SQL_OVERRIDE");

    assertEquals("SQL_OVERRIDE", actualSqls.get("columns-au.csv"));
    assertEquals("SQL_OVERRIDE", actualSqls.get("columns.csv"));
  }

  @Test
  public void connector_generatesExpectedSql_withWhereOverrides() throws IOException {
    Map<String, String> actualSqls =
        collectSqlStatements("-Dsnowflake.metadata.columns.where=SQL_OVERRIDE");

    assertTrue(actualSqls.get("columns-au.csv").endsWith("WHERE SQL_OVERRIDE"));
    assertFalse(actualSqls.get("columns-au.csv").contains("WHERE DELETED IS NULL"));
    assertEquals(1, StringUtils.countMatches(actualSqls.get("columns-au.csv"), " WHERE "));

    assertTrue(actualSqls.get("columns.csv").endsWith("WHERE SQL_OVERRIDE"));
    assertEquals(1, StringUtils.countMatches(actualSqls.get("columns.csv"), " WHERE "));
  }

  @Test
  public void connector_generatesExpectedSql_withDatabaseFilter() throws IOException {
    Map<String, String> actualSqls = collectSqlStatements("--database", "db1");

    assertEquals(
        "SELECT catalog_name, schema_name FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA WHERE DELETED IS NULL AND catalog_name IN ('DB1')",
        actualSqls.get("schemata-au.csv"));
    assertEquals(
        "SELECT catalog_name, schema_name FROM db1.INFORMATION_SCHEMA.SCHEMATA",
        actualSqls.get("schemata.csv"));
    assertEquals("SHOW EXTERNAL TABLES IN DATABASE \"DB1\"", actualSqls.get("external_tables.csv"));
  }

  @Test
  public void connector_generatesExpectedSql_withDatabaseFilterAndWhereOverride()
      throws IOException {
    ImmutableMultimap<String, String> actualSqls =
        collectSqlStatementsAsMultimap(
            "--database", "db1,db2", "-Dsnowflake.metadata.schemata.where=SQL_OVERRIDE");

    assertEquals(
        ImmutableList.of(
            "SELECT catalog_name, schema_name FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA WHERE SQL_OVERRIDE"),
        actualSqls.get("schemata-au.csv"));
    assertEquals(
        ImmutableList.of(
            "SELECT catalog_name, schema_name FROM db1.INFORMATION_SCHEMA.SCHEMATA WHERE SQL_OVERRIDE",
            "SELECT catalog_name, schema_name FROM db2.INFORMATION_SCHEMA.SCHEMATA WHERE SQL_OVERRIDE"),
        actualSqls.get("schemata.csv"));

    // Two SHOW commands are executed and the result is appended to the same output file.
    assertEquals(
        ImmutableList.of(
            "SHOW EXTERNAL TABLES IN DATABASE \"DB1\"", "SHOW EXTERNAL TABLES IN DATABASE \"DB2\""),
        actualSqls.get("external_tables.csv"));
  }

  @Test
  public void connector_generatesExpectedSql_withSchemaFilter() throws IOException {
    Map<String, String> actualSqls = collectSqlStatements("--schema", "schema1,schema2");

    assertEquals(
        "SELECT catalog_name, schema_name FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA WHERE DELETED IS NULL AND schema_name IN ('SCHEMA1', 'SCHEMA2')",
        actualSqls.get("schemata-au.csv"));
    assertEquals(
        "SELECT catalog_name, schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE schema_name IN ('SCHEMA1', 'SCHEMA2')",
        actualSqls.get("schemata.csv"));
    assertEquals(
        "SELECT table_catalog, table_schema, table_name, table_type, row_count, bytes, clustering_key FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES WHERE DELETED IS NULL AND table_schema IN ('SCHEMA1', 'SCHEMA2')",
        actualSqls.get("tables-au.csv"));
    assertEquals(
        "SELECT table_catalog, table_schema, table_name, table_type, row_count, bytes, clustering_key FROM INFORMATION_SCHEMA.TABLES WHERE table_schema IN ('SCHEMA1', 'SCHEMA2')",
        actualSqls.get("tables.csv"));
    assertEquals(
        "SELECT table_catalog, table_schema, table_name, ordinal_position, column_name, data_type, is_nullable, column_default, character_maximum_length, numeric_precision, numeric_scale, datetime_precision, comment FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS WHERE DELETED IS NULL AND table_schema IN ('SCHEMA1', 'SCHEMA2')",
        actualSqls.get("columns-au.csv"));
    assertEquals(
        "SELECT table_catalog, table_schema, table_name, ordinal_position, column_name, data_type, is_nullable, column_default, character_maximum_length, numeric_precision, numeric_scale, datetime_precision, comment FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema IN ('SCHEMA1', 'SCHEMA2')",
        actualSqls.get("columns.csv"));
    assertEquals(
        "SELECT function_schema, function_name, data_type, argument_signature FROM SNOWFLAKE.ACCOUNT_USAGE.FUNCTIONS WHERE DELETED IS NULL AND function_schema IN ('SCHEMA1', 'SCHEMA2')",
        actualSqls.get("functions-au.csv"));
    assertEquals(
        "SELECT function_schema, function_name, data_type, argument_signature FROM INFORMATION_SCHEMA.FUNCTIONS WHERE function_schema IN ('SCHEMA1', 'SCHEMA2')",
        actualSqls.get("functions.csv"));
    assertEquals("SHOW EXTERNAL TABLES", actualSqls.get("external_tables.csv"));
  }

  @Test
  public void connector_generatesExpectedSql_withDatabaseAndSchemaFilter() throws IOException {
    ImmutableMultimap<String, String> actualSqls =
        collectSqlStatementsAsMultimap("--database", "db1,db2", "--schema", "schema1,schema2");

    assertEquals(
        ImmutableList.of(
            "SELECT catalog_name, schema_name FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA WHERE DELETED IS NULL AND catalog_name IN ('DB1', 'DB2') AND schema_name IN ('SCHEMA1', 'SCHEMA2')"),
        actualSqls.get("schemata-au.csv"));
    assertEquals(
        ImmutableList.of(
            "SELECT catalog_name, schema_name FROM db1.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name IN ('SCHEMA1', 'SCHEMA2')",
            "SELECT catalog_name, schema_name FROM db2.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name IN ('SCHEMA1', 'SCHEMA2')"),
        actualSqls.get("schemata.csv"));
    assertEquals(
        ImmutableList.of(
            "SELECT table_catalog, table_schema, table_name, table_type, row_count, bytes, clustering_key FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES WHERE DELETED IS NULL AND table_catalog IN ('DB1', 'DB2') AND table_schema IN ('SCHEMA1', 'SCHEMA2')"),
        actualSqls.get("tables-au.csv"));
    assertEquals(
        ImmutableList.of(
            "SELECT table_catalog, table_schema, table_name, table_type, row_count, bytes, clustering_key FROM db1.INFORMATION_SCHEMA.TABLES WHERE table_schema IN ('SCHEMA1', 'SCHEMA2')",
            "SELECT table_catalog, table_schema, table_name, table_type, row_count, bytes, clustering_key FROM db2.INFORMATION_SCHEMA.TABLES WHERE table_schema IN ('SCHEMA1', 'SCHEMA2')"),
        actualSqls.get("tables.csv"));
    assertEquals(
        ImmutableList.of(
            "SELECT table_catalog, table_schema, table_name, ordinal_position, column_name, data_type, is_nullable, column_default, character_maximum_length, numeric_precision, numeric_scale, datetime_precision, comment FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS WHERE DELETED IS NULL AND table_catalog IN ('DB1', 'DB2') AND table_schema IN ('SCHEMA1', 'SCHEMA2')"),
        actualSqls.get("columns-au.csv"));
    assertEquals(
        ImmutableList.of(
            "SELECT table_catalog, table_schema, table_name, ordinal_position, column_name, data_type, is_nullable, column_default, character_maximum_length, numeric_precision, numeric_scale, datetime_precision, comment FROM db1.INFORMATION_SCHEMA.COLUMNS WHERE table_schema IN ('SCHEMA1', 'SCHEMA2')",
            "SELECT table_catalog, table_schema, table_name, ordinal_position, column_name, data_type, is_nullable, column_default, character_maximum_length, numeric_precision, numeric_scale, datetime_precision, comment FROM db2.INFORMATION_SCHEMA.COLUMNS WHERE table_schema IN ('SCHEMA1', 'SCHEMA2')"),
        actualSqls.get("columns.csv"));
    assertEquals(
        ImmutableList.of(
            "SELECT function_schema, function_name, data_type, argument_signature FROM SNOWFLAKE.ACCOUNT_USAGE.FUNCTIONS WHERE DELETED IS NULL AND function_catalog IN ('DB1', 'DB2') AND function_schema IN ('SCHEMA1', 'SCHEMA2')"),
        actualSqls.get("functions-au.csv"));
    assertEquals(
        ImmutableList.of(
            "SELECT function_schema, function_name, data_type, argument_signature FROM db1.INFORMATION_SCHEMA.FUNCTIONS WHERE function_schema IN ('SCHEMA1', 'SCHEMA2')",
            "SELECT function_schema, function_name, data_type, argument_signature FROM db2.INFORMATION_SCHEMA.FUNCTIONS WHERE function_schema IN ('SCHEMA1', 'SCHEMA2')"),
        actualSqls.get("functions.csv"));
    assertEquals(
        ImmutableList.of(
            "SHOW EXTERNAL TABLES IN DATABASE \"DB1\"", "SHOW EXTERNAL TABLES IN DATABASE \"DB2\""),
        actualSqls.get("external_tables.csv"));
  }

  @Test
  public void identifierNameStringLiteral() {
    assertEquals("'ABC'", SnowflakeMetadataConnector.identifierNameStringLiteral("abc"));
    assertEquals("'abc'", SnowflakeMetadataConnector.identifierNameStringLiteral("\"abc\""));

    assertEquals("''''", SnowflakeMetadataConnector.identifierNameStringLiteral("'"));
    assertEquals("''''", SnowflakeMetadataConnector.identifierNameStringLiteral("\"'\""));

    assertEquals("'A''C\"'", SnowflakeMetadataConnector.identifierNameStringLiteral("a'c\""));
    assertEquals("'a''c\"'", SnowflakeMetadataConnector.identifierNameStringLiteral("\"a'c\"\""));
  }

  @Test
  public void identifierNameQuoted() {
    assertEquals("\"ABC\"", SnowflakeMetadataConnector.identifierNameQuoted("abc"));
    assertEquals("\"abc\"", SnowflakeMetadataConnector.identifierNameQuoted("\"abc\""));

    assertEquals("\"'\"", SnowflakeMetadataConnector.identifierNameQuoted("'"));
    assertEquals("\"'\"", SnowflakeMetadataConnector.identifierNameQuoted("\"'\""));

    assertEquals("\"A'C\"\"\"", SnowflakeMetadataConnector.identifierNameQuoted("a'c\""));
    assertEquals("\"a'c\"\"\"", SnowflakeMetadataConnector.identifierNameQuoted("\"a'c\"\""));
  }

  private static ImmutableMultimap<String, String> collectSqlStatementsAsMultimap(
      String... extraArgs) throws IOException {
    List<Task<?>> tasks = new ArrayList<>();
    SnowflakeMetadataConnector connector = new SnowflakeMetadataConnector();
    ImmutableList<String> standardArgs = ImmutableList.of("--connector", connector.getName());
    ArrayList<String> args = new ArrayList<>(standardArgs);
    for (String item : extraArgs) {
      args.add(item);
    }
    connector.addTasksTo(tasks, ConnectorArguments.create(args));
    ImmutableMultimap.Builder<String, String> builder = ImmutableMultimap.builder();
    tasks.stream()
        .filter(t -> t instanceof JdbcSelectTask)
        .map(t -> (JdbcSelectTask) t)
        .forEach(t -> builder.put(t.getName(), t.getSql()));
    return builder.build();
  }

  private static ImmutableMap<String, String> collectSqlStatements(String... extraArgs)
      throws IOException {
    return collectSqlStatementsAsMultimap(extraArgs).entries().stream()
        .collect(
            ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue, (first, dup) -> first));
  }
}
