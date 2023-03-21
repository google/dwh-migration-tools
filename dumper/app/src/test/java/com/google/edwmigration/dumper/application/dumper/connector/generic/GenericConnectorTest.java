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
package com.google.edwmigration.dumper.application.dumper.connector.generic;

import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorExecutionTest;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ./gradlew compilerworks-application-dumper:{clean,test} -Dtest.verbose=true
 * -Dtest-sys-prop.test.dumper=true --tests GenericConnectorTest
 */
@RunWith(JUnit4.class)
public class GenericConnectorTest extends AbstractConnectorExecutionTest {

  private static final Logger LOG = LoggerFactory.getLogger(GenericConnectorTest.class);

  private static final String SUBPROJECT = "compilerworks-application-dumper";

  private static final File GENERIC_DUMPER_TEST =
      new File(TestUtils.getTestResourcesDir(SUBPROJECT), "dumper-test/generic.sql");

  @Test
  public void testGeneric() throws IOException, Exception {

    File outputFile = TestUtils.newOutputFile("compilerworks-generic-logs.zip");

    run(
        "--connector",
        "generic",
        "--jdbcDriver",
        "org.postgresql.Driver",
        "--url",
        "jdbc:postgresql:cw",
        "--user",
        "cw",
        "--password",
        "password",
        "--generic-query",
        "select 1",
        "--output",
        outputFile.getAbsolutePath(),
        "--sqlscript",
        GENERIC_DUMPER_TEST.getAbsolutePath());

    File outputFile1 = TestUtils.newOutputFile("compilerworks-generic-logs-1.zip");

    run(
        "--connector",
        "generic",
        "--jdbcDriver",
        "org.postgresql.Driver",
        "--url",
        "jdbc:postgresql:cw",
        "--user",
        "cw",
        "--password",
        "password",
        "--query-log-start",
        "2019-12-25",
        "--query-log-end",
        "2019-12-31",
        "--generic-query",
        "select * from test.generic_logs  where log_timestamp>= {period-start} AND log_timestamp< {period-end}",
        "--output",
        outputFile1.getAbsolutePath(),
        "--sqlscript",
        GENERIC_DUMPER_TEST.getAbsolutePath());
  }
}
