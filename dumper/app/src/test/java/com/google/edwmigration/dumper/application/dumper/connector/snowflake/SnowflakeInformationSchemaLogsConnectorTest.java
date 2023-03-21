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

import com.google.common.base.Predicates;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeLogsDumpFormat;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author shevek */
@RunWith(JUnit4.class)
public class SnowflakeInformationSchemaLogsConnectorTest
    extends AbstractSnowflakeConnectorExecutionTest {

  private final LogsConnector connector = new SnowflakeInformationSchemaLogsConnector();

  @Test
  public void testExecution() throws Exception {
    File outputFile = TestUtils.newOutputFile("compilerworks-snowflake-logs-is.zip");
    if (!run(ARGS(connector, outputFile))) return;

    ZipValidator validator = new ZipValidator().withFormat(SnowflakeLogsDumpFormat.FORMAT_NAME);

    validator.withAllowedEntries(Predicates.alwaysTrue()); // Permit any files.

    validator.run(outputFile);
  }
}
