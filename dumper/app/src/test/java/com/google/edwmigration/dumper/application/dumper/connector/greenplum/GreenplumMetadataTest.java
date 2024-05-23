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
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Greenplum metadata connector tests */
@RunWith(JUnit4.class)
public class GreenplumMetadataTest extends AbstractConnectorExecutionTest {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(GreenplumMetadataTest.class);

  private static final String SUBPROJECT = "compilerworks-application-dumper";

  private static final File PG_DUMPER_TEST = new File(TestUtils.getTestResourcesDir(SUBPROJECT),
      "dumper-test/postgresql.sql");

  private final MetadataConnector connector = new GreenplumMetadataConnector();

  @Test
  public void testConnector() throws Exception {
    testConnectorDefaults(connector);
  }

  @Test
  public void testMetadata() throws Exception {
    Assume.assumeTrue(isDumperTest());

    File outputFile = TestUtils.newOutputFile("compilerworks-greenplum-metadata.zip");
    LOG.debug("Output file: {}", outputFile.getAbsolutePath());

    runDumper(
        "--connector",
        connector.getName(),
        "--user",
        "cw",
        "--password",
        "password",
        "--output",
        outputFile.getAbsolutePath(),
        "--sqlscript",
        PG_DUMPER_TEST.getAbsolutePath());
  }
}
