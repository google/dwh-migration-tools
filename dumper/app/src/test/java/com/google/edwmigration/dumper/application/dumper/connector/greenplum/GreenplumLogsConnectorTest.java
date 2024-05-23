/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.greenplum;

import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorExecutionTest;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.GreenplumLogsDumpFormat;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author zzwang */
@RunWith(JUnit4.class)
public class GreenplumLogsConnectorTest extends AbstractConnectorExecutionTest {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(GreenplumLogsConnectorTest.class);

  private final GreenplumLogsConnector connector = new GreenplumLogsConnector();

  @Test
  public void testConnector() throws Exception {
    testConnectorDefaults(connector);
  }

  @Test
  public void testExecution() throws Exception {
    Assume.assumeTrue(isDumperTest());

    File outputFile = TestUtils.newOutputFile("test-compilerworks-greenplum.zip");
    LOG.debug("Output file: {}", outputFile.getAbsolutePath());

    runDumper("--connector", connector.getName(), "--output", outputFile.getAbsolutePath());

    ZipValidator validator = new ZipValidator().withFormat(GreenplumLogsDumpFormat.FORMAT_NAME);
    validator.run(outputFile);
  }
}
