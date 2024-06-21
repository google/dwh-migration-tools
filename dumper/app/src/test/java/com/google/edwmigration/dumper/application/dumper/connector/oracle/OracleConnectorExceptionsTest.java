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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OracleConnectorExceptionsTest {

  @Test
  public void invalidSqlSourcePath_success_correctMessage() {
    Exception exception =
        OracleConnectorExceptions.invalidSqlSourcePath("oracle-stats/native/invalid-filename.sql");
    assertEquals(
        "Invalid path for SQL source file: oracle-stats/native/invalid-filename.sql",
        exception.getMessage());
  }

  @Test
  public void mustProvideServiceOrSid_success_correctMessage() {
    Exception exception = OracleConnectorExceptions.mustProvideServiceOrSid();
    assertEquals(
        "Provide either -oracle-service or -oracle-sid for oracle dumper", exception.getMessage());
  }
}
