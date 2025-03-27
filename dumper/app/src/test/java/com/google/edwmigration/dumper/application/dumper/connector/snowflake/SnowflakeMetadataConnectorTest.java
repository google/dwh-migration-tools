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

import com.google.common.collect.ImmutableMap;
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
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.junit.Assert;
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
        Assert.assertThrows(
            MetadataDumperUsageException.class,
            () -> {
              File outputFile =
                  TestUtils.newOutputFile("compilerworks-snowflake-metadata-fail.zip");
              String[] args = ARGS(connector, outputFile);

              Assert.assertEquals("--database", args[6]);
              args[7] = args[7] + "_NOT_EXISTS";
              run(args);
            });

    Assert.assertTrue(exception.getMessage().startsWith("Database name not found"));
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

    Assert.assertEquals(expectedSqls.size(), actualSqls.size());
    Assert.assertEquals(expectedSqls.keySet(), actualSqls.keySet());
    for (String name : expectedSqls.keySet()) {
      Assert.assertEquals(expectedSqls.get(name), actualSqls.get(name));
    }
  }

  private static Map<String, String> collectSqlStatements() throws IOException {
    List<Task<?>> tasks = new ArrayList<>();
    SnowflakeMetadataConnector connector = new SnowflakeMetadataConnector();
    connector.addTasksTo(tasks, new ConnectorArguments("--connector", connector.getName()));
    return tasks.stream()
        .filter(t -> t instanceof JdbcSelectTask)
        .map(t -> (JdbcSelectTask) t)
        .collect(ImmutableMap.toImmutableMap(Task::getName, JdbcSelectTask::getSql));
  }

  static class TaskSqlMap extends HashMap<String, String> {}
}
