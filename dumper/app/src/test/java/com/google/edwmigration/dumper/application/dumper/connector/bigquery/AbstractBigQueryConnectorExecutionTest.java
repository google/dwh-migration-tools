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
package com.google.edwmigration.dumper.application.dumper.connector.bigquery;

import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorExecutionTest;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author matt */
public abstract class AbstractBigQueryConnectorExecutionTest
    extends AbstractConnectorExecutionTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractBigQueryConnectorExecutionTest.class);

  @ClassRule
  public static final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @BeforeClass
  // TODO("The alternative to this is simply to set the environment variable in the gradle build
  // file.")
  public static void setEnvironmentVariables() {
    File credentialsFile =
        new File(
            TestUtils.getProjectRootDir(),
            "compilerworks-plugin-test-bigquery/build/resources/main/api-token.json");
    LOG.info("Using BigQuery credentials: {}", credentialsFile.getAbsolutePath());
    environmentVariables.set("GOOGLE_APPLICATION_CREDENTIALS", credentialsFile.getAbsolutePath());
  }
}
