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
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeLogsConnector.TimeSeriesView.valuesInOrder;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeLogsConnector.addOverridesToQuery;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeLogsConnector.earliestTimestamp;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeLogsConnector.formatPrefix;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeLogsConnector.overrideableQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeLogsConnector.TimeSeriesView;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
import org.junit.Test;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/** @author shevek */
@RunWith(Theories.class)
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
  public void addOverridesToQuery_fullQueryOverride_success() {
    String override =
        "SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY\n"
            + "WHERE end_time >= to_timestamp_ltz('%s')\n"
            + "AND end_time <= to_timestamp_ltz('%s')\n";
    String property = String.format("-Dsnowflake.logs.query=%s", override);
    ConnectorArguments arguments =
        ConnectorArguments.create(ImmutableList.of("--connector", "snowflake-logs", property));

    String result = addOverridesToQuery(arguments, "SELECT 1");

    assertEquals(override, result);
  }

  @Test
  public void addOverridesToQuery_fullQueryOverrideWithBadValue_throwsException() {
    String override = "text_with_no_format_specifiers";
    String property = String.format("-Dsnowflake.logs.query=%s", override);
    ConnectorArguments arguments =
        ConnectorArguments.create(ImmutableList.of("--connector", "snowflake-logs", property));

    assertThrows(
        MetadataDumperUsageException.class, () -> addOverridesToQuery(arguments, "SELECT 1"));
  }

  @Test
  public void addOverridesToQuery_noOverrides_nothingChanges() {
    String originalQuery = "SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE 1=1\n";
    ConnectorArguments arguments =
        ConnectorArguments.create(ImmutableList.of("--connector", "snowflake-logs"));

    String result = addOverridesToQuery(arguments, originalQuery);

    assertEquals(originalQuery, result);
  }

  @Test
  public void addOverridesToQuery_whereOverride_success() {
    String originalQuery = "SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE 1=1\n";
    String override = "rows_inserted > 0";
    String property = String.format("-Dsnowflake.logs.where=%s", override);
    ConnectorArguments arguments =
        ConnectorArguments.create(ImmutableList.of("--connector", "snowflake-logs", property));

    String result = addOverridesToQuery(arguments, originalQuery);

    assertTrue(result, result.contains(originalQuery));
    assertTrue(result, result.contains("AND"));
    assertTrue(result, result.contains(override));
  }

  @Test
  public void earliestTimestamp_notProvided_emptyResult() {
    ConnectorArguments arguments =
        ConnectorArguments.create(ImmutableList.of("--connector", "snowflake-logs"));

    String result = earliestTimestamp(arguments);

    assertEquals("", result);
  }

  @Test
  public void earliestTimestamp_provided_resultMatches() {
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "snowflake-logs",
                "--" + ConnectorArguments.OPT_QUERY_LOG_EARLIEST_TIMESTAMP,
                "2024-03-21"));

    String result = earliestTimestamp(arguments);

    assertTrue(result, result.contains("2024-03-21"));
    assertTrue(result, result.contains("start_time"));
    assertTrue(result, result.endsWith("\n"));
  }

  enum TestEnum {
    FirstValue,
    SecondValue;
  };

  @Test
  public void formatPrefix_success() {

    String result = formatPrefix(TestEnum.class, "TASK_HISTORY");

    assertEquals(
        "SELECT FIRST_VALUE, SECOND_VALUE FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY", result);
  }

  @Test
  public void overrideableQuery_overrideAbsent_defaultUsed() {
    String defaultSql = "SELECT event_name, query_id FROM WAREHOUSE_EVENTS_HISTORY";

    String result = overrideableQuery(null, defaultSql, "timestamp");

    assertTrue(result, result.contains("event_name"));
  }

  @Test
  public void overrideableQuery_overrideEmpty_resultEmpty() {
    String defaultSql = "SELECT event_name, query_id FROM WAREHOUSE_EVENTS_HISTORY";
    String override = "";

    String result = overrideableQuery(override, defaultSql, "timestamp");

    assertFalse(result, result.contains("event_name"));
  }

  @Test
  public void overrideableQuery_overridePresent_defaultIgnored() {
    String defaultSql = "SELECT event_name, query_id FROM WAREHOUSE_EVENTS_HISTORY";
    String override = "SELECT query_id FROM WAREHOUSE_EVENTS_HISTORY";

    String result = overrideableQuery(override, defaultSql, "timestamp");

    assertFalse(result, result.contains("event_name"));
  }

  @Test
  public void validate_unsupportedOption_throwsException() {
    SnowflakeLogsConnector connector = new SnowflakeLogsConnector();
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "snowflake-logs",
                "--assessment",
                "--" + ConnectorArguments.OPT_QUERY_LOG_EARLIEST_TIMESTAMP,
                "2024"));

    assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
  }

  @Theory
  public void valuesInOrder_allValuesPresent(TimeSeriesView view) {

    assertTrue(valuesInOrder.contains(view));
  }

  @Test
  public void valuesInOrder_sizeMatches() {
    assertEquals(TimeSeriesView.values().length, valuesInOrder.size());
  }
}
