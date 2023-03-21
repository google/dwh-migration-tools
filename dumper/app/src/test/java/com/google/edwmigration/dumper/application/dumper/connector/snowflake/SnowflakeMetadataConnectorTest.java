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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
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
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeMetadataConnectorTest.class);

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
      LOG.debug("Output file: {}", outputFile.getAbsolutePath());

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

      validator.run(outputFile);
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
}
