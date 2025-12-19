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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
@RunWith(JUnit4.class)
public class SnowflakeMetadataConnectorTest extends AbstractSnowflakeConnectorExecutionTest {

  @SuppressWarnings("UnusedVariable")
  private static final Logger logger =
      LoggerFactory.getLogger(SnowflakeMetadataConnectorTest.class);

  private static final String FEATURES_CSV = "features.csv";
  private static final String ACCOUNT_USAGE_SIMPLE_FILE = "account-usage-simple.sql";
  private static final String ACCOUNT_USAGE_COMPLEX_FILE = "account-usage-complex.sql";
  private static final String SHOW_BASED_FILE = "show-based.sql";
  private static final String SNOWFLAKE_FEATURES_PREFIX = "snowflake-features/";

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
  public void connector_generatesExpectedSql() throws IOException {
    Map<String, String> actualSqls = collectSqlStatements();
    TaskSqlMap expectedSqls =
        CoreMetadataDumpFormat.MAPPER.readValue(
            Resources.toString(
                Resources.getResource("connector/snowflake/jdbc-tasks-sql.yaml"),
                StandardCharsets.UTF_8),
            TaskSqlMap.class);

    // ignore feature.csv, it will be tested in separate test because the query is too complex
    int actualSizeWithoutFeatures = actualSqls.size() - 1;
    Set<String> actualFileNamesWithoutFeatures = new HashSet<>(actualSqls.keySet());
    actualFileNamesWithoutFeatures.remove(FEATURES_CSV);

    assertEquals(expectedSqls.size(), actualSizeWithoutFeatures);
    assertEquals(expectedSqls.keySet(), actualFileNamesWithoutFeatures);
    for (String name : expectedSqls.keySet()) {
      assertEquals(expectedSqls.get(name), actualSqls.get(name));
    }
  }

  @Test
  public void connector_checkExpectedFeaturesQueryFilesExist() {
    loadFile(SNOWFLAKE_FEATURES_PREFIX + ACCOUNT_USAGE_SIMPLE_FILE);
    loadFile(SNOWFLAKE_FEATURES_PREFIX + ACCOUNT_USAGE_COMPLEX_FILE);
    loadFile(SNOWFLAKE_FEATURES_PREFIX + SHOW_BASED_FILE);
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
        "SELECT catalog_name, schema_name FROM db1.INFORMATION_SCHEMA.SCHEMATA WHERE catalog_name IN ('DB1')",
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
            "SELECT catalog_name, schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE SQL_OVERRIDE"),
        actualSqls.get("schemata.csv"));

    // Two SHOW commands are executed and the result is appended to the same output file.
    assertEquals(
        ImmutableList.of(
            "SHOW EXTERNAL TABLES IN DATABASE \"DB1\"", "SHOW EXTERNAL TABLES IN DATABASE \"DB2\""),
        actualSqls.get("external_tables.csv"));
  }

  @Test
  public void databaseNameStringLiteral() {
    assertEquals("'ABC'", SnowflakeMetadataConnector.databaseNameStringLiteral("abc"));
    assertEquals("'abc'", SnowflakeMetadataConnector.databaseNameStringLiteral("\"abc\""));

    assertEquals("''''", SnowflakeMetadataConnector.databaseNameStringLiteral("'"));
    assertEquals("''''", SnowflakeMetadataConnector.databaseNameStringLiteral("\"'\""));

    assertEquals("'A''C\"'", SnowflakeMetadataConnector.databaseNameStringLiteral("a'c\""));
    assertEquals("'a''c\"'", SnowflakeMetadataConnector.databaseNameStringLiteral("\"a'c\"\""));
  }

  @Test
  public void databaseNameQuoted() {
    assertEquals("\"ABC\"", SnowflakeMetadataConnector.databaseNameQuoted("abc"));
    assertEquals("\"abc\"", SnowflakeMetadataConnector.databaseNameQuoted("\"abc\""));

    assertEquals("\"'\"", SnowflakeMetadataConnector.databaseNameQuoted("'"));
    assertEquals("\"'\"", SnowflakeMetadataConnector.databaseNameQuoted("\"'\""));

    assertEquals("\"A'C\"\"\"", SnowflakeMetadataConnector.databaseNameQuoted("a'c\""));
    assertEquals("\"a'c\"\"\"", SnowflakeMetadataConnector.databaseNameQuoted("\"a'c\"\""));
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

  static class TaskSqlMap extends HashMap<String, String> {}

  private void loadFile(String path) {
    try {
      Resources.toString(Resources.getResource(path), UTF_8);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format("An invalid file was provided: '%s'.", path), e);
    }
  }
}
