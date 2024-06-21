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

import static com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource.NATIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OracleStatsQueryTest {

  @Test
  public void description_success() throws IOException {
    OracleStatsQuery query = OracleStatsQuery.create("db-objects", NATIVE);
    assertEquals("Query{name=db-objects, statsSource=NATIVE}", query.description());
  }

  @Test
  public void create_fileDoesNotExist_throwsException() {

    // Act
    ThrowingRunnable runnable = () -> OracleStatsQuery.create("invalid-filename", NATIVE);
    Exception exception = assertThrows(IllegalArgumentException.class, runnable);

    // Assert
    String message = exception.getMessage();
    assertTrue("Actual: " + message, message.startsWith("Invalid path for SQL source file: "));
  }
}
