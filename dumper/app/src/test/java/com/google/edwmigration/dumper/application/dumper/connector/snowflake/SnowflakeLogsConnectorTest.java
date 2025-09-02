/*
 * Copyright 2022-2025 Google LLC
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

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeLogsConnector.earliestTimestamp;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeLogsConnector.overrideableQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author shevek */
@RunWith(JUnit4.class)
public class SnowflakeLogsConnectorTest {

  @Test
  public void testExecution() throws Exception {
    class ExecutionTest extends AbstractSnowflakeConnectorExecutionTest {
      void test(File output) throws Exception {
        if (run(ARGS(new SnowflakeLogsConnector(), output))) {
          new ZipValidator()
              .withFormat("snowflake.logs.zip")
              .withAllowedEntries(alwaysTrue())
              .run(output);
        }
      }
    }
    File outputFile = TestUtils.newOutputFile("compilerworks-snowflake-logs-au.zip");

    new ExecutionTest().test(outputFile);
  }

  @Test
  public void earliestTimestamp_notProvided_emptyResult() throws IOException {
    ConnectorArguments arguments = new ConnectorArguments("--connector", "snowflake-logs");

    String result = earliestTimestamp(arguments);

    assertEquals("", result);
  }

  @Test
  public void earliestTimestamp_provided_resultMatches() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "snowflake-logs",
            "--" + ConnectorArguments.OPT_QUERY_LOG_EARLIEST_TIMESTAMP,
            "2024-03-21");

    String result = earliestTimestamp(arguments);

    assertTrue(result, result.contains("2024-03-21"));
    assertTrue(result, result.contains("start_time"));
    assertTrue(result, result.endsWith("\n"));
  }

  @Test
  public void overrideableQuery_overrideAbsent_defaultUsed() throws IOException {
    String defaultSql = "SELECT event_name, query_id FROM WAREHOUSE_EVENTS_HISTORY";

    String result = overrideableQuery(null, defaultSql, "timestamp");

    assertTrue(result, result.contains("event_name"));
  }

  @Test
  public void overrideableQuery_overrideEmpty_resultEmpty() throws IOException {
    String defaultSql = "SELECT event_name, query_id FROM WAREHOUSE_EVENTS_HISTORY";
    String override = "";

    String result = overrideableQuery(override, defaultSql, "timestamp");

    assertFalse(result, result.contains("event_name"));
  }

  @Test
  public void overrideableQuery_overridePresent_defaultIgnored() throws IOException {
    String defaultSql = "SELECT event_name, query_id FROM WAREHOUSE_EVENTS_HISTORY";
    String override = "SELECT query_id FROM WAREHOUSE_EVENTS_HISTORY";

    String result = overrideableQuery(override, defaultSql, "timestamp");

    assertFalse(result, result.contains("event_name"));
  }

  @Test
  public void validate_unsupportedOption_throwsException() throws IOException {
    SnowflakeLogsConnector connector = new SnowflakeLogsConnector();
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "snowflake-logs",
            "--assessment",
            "--" + ConnectorArguments.OPT_QUERY_LOG_EARLIEST_TIMESTAMP,
            "2024");

    Assert.assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
  }
}
