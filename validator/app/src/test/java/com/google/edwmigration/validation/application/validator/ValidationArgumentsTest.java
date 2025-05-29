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
package com.google.edwmigration.validation.application.validator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.runners.model.MultipleFailureException.assertEmpty;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author nehanene */
@RunWith(JUnit4.class)
public class ValidationArgumentsTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final String JSON_SAMPLE_CONN =
      "{\n"
          + "  \"connectionType\": \"teradata\",\n"
          + "  \"database\": \"db\",\n"
          + "  \"driver\": \"teradata.jar\",\n"
          + "  \"user\": \"user\",\n"
          + "  \"pass\": \"pass\",\n"
          + "  \"host\": \"localhost\"\n"
          + "}";

  @Test
  public void getSourceConnectionTest() throws Exception {
    File connFile = temporaryFolder.newFile("teradata.json");
    String connFilePath = connFile.getAbsolutePath();
    Files.write(connFile.toPath(), JSON_SAMPLE_CONN.getBytes(StandardCharsets.UTF_8));

    ValidationArguments arguments =
        new ValidationArguments(
            "--source-connection",
            connFilePath,
            "--target-connection",
            "bigquery.json",
            "--table",
            "table",
            "--bq-staging-dataset",
            "dataset",
            "--gcs-path",
            "path",
            "--bq-results-table",
            "table",
            "--bq-staging-dataset",
            "dataset",
            "--primary-keys",
            "pk");
    Path sourceConnectionPath = arguments.getSourceConnectionPath();
    ValidationConnection sourceConn = arguments.getSourceConnection();

    assertEquals(Paths.get(connFilePath), sourceConnectionPath);
    assertEquals(sourceConn.getDatabase(), "db");
    assertEquals(sourceConn.getDriverPaths(), Arrays.asList("teradata.jar"));
    assertNull(sourceConn.getProjectId());
  }

  @Test
  public void testGetColumnMappingsEmpty() {
    ValidationArguments arguments =
        new ValidationArguments(
            "--source-connection",
            "path.json",
            "--target-connection",
            "bigquery.json",
            "--table",
            "--bq-staging-dataset",
            "dataset",
            "--gcs-path",
            "path",
            "--bq-results-table",
            "table",
            "--bq-staging-dataset",
            "dataset",
            "--primary-keys",
            "pk")
        ;
    assertNotNull(arguments.getColumnMappings());
  }

}
