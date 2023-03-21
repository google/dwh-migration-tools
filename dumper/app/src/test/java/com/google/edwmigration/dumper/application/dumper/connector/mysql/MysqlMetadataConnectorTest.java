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
package com.google.edwmigration.dumper.application.dumper.connector.mysql;

import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorExecutionTest;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.MysqlMetadataDumpFormat;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
@RunWith(JUnit4.class)
public class MysqlMetadataConnectorTest extends AbstractConnectorExecutionTest {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(MysqlMetadataConnectorTest.class);

  private final MetadataConnector connector = new MysqlMetadataConnector();

  @Test
  public void testConnector() throws Exception {
    testConnectorDefaults(connector);
  }

  @Ignore("Expensive. Move to integration tests?")
  @Test
  public void testExecution() throws Exception {
    File outputFile = TestUtils.newOutputFile("compilerworks-mysql-metadata.zip");
    LOG.debug("Output file: {}", outputFile.getAbsolutePath());

    // TODO: Constants from MysqlValidator.
    runDumper(
        "--connector",
        connector.getName(),
        "--user",
        "cw",
        "--password",
        "password",
        "--output",
        outputFile.getAbsolutePath());

    ZipValidator validator = new ZipValidator().withFormat(MysqlMetadataDumpFormat.FORMAT_NAME);

    validator.withEntryValidator(
        MysqlMetadataDumpFormat.SchemataFormat.ZIP_ENTRY_NAME,
        MysqlMetadataDumpFormat.SchemataFormat.Header.class);
    validator.withEntryValidator(
        MysqlMetadataDumpFormat.TablesFormat.ZIP_ENTRY_NAME,
        MysqlMetadataDumpFormat.TablesFormat.Header.class);
    validator.withEntryValidator(
        MysqlMetadataDumpFormat.ColumnsFormat.ZIP_ENTRY_NAME,
        MysqlMetadataDumpFormat.ColumnsFormat.Header.class);
    validator.withEntryValidator(
        MysqlMetadataDumpFormat.ViewsFormat.ZIP_ENTRY_NAME,
        MysqlMetadataDumpFormat.ViewsFormat.Header.class);
    validator.withEntryValidator(
        MysqlMetadataDumpFormat.FunctionsFormat.ZIP_ENTRY_NAME,
        MysqlMetadataDumpFormat.FunctionsFormat.Header.class);

    validator.run(outputFile);
  }
}
