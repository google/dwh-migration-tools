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
package com.google.edwmigration.dumper.application.dumper;

import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_HOST_DEFAULT;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_QUERY_LOG_EARLIEST_TIMESTAMP;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_QUERY_LOG_START;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConnectorArgumentsTest {

  @Test
  public void isPasswordProvided_success() throws Exception {
    {
      ConnectorArguments arguments = new ConnectorArguments("--connector", "simple", "--password");
      assertTrue(arguments.isPasswordFlagProvided());
    }
    {
      ConnectorArguments arguments =
          new ConnectorArguments("--connector", "simple", "--password", "secret");
      assertTrue(arguments.isPasswordFlagProvided());
    }
  }

  @Test
  public void isPasswordProvided_false() throws Exception {
    ConnectorArguments arguments = new ConnectorArguments("--connector", "simple");
    assertFalse(arguments.isPasswordFlagProvided());
  }

  @Test
  public void getDatabases_success() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments("--connector", "teradata", "--database", "sample-db");

    List<String> databaseNames = arguments.getDatabases();

    assertEquals(ImmutableList.of("sample-db"), databaseNames);
  }

  @Test
  public void getDatabases_databaseOptionNotSpecified_success() throws IOException {
    ConnectorArguments arguments = new ConnectorArguments("--connector", "teradata");

    List<String> databaseNames = arguments.getDatabases();

    assertTrue(databaseNames.isEmpty());
  }

  @Test
  public void getDatabases_trimDatabaseNames() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments("--connector", "teradata", "--database", "db1, db2 ");

    List<String> databaseNames = arguments.getDatabases();

    assertEquals(ImmutableList.of("db1", "db2"), databaseNames);
  }

  @Test
  public void getDatabases_trimDatabaseNamesFilteringOutBlankStrings() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments("--connector", "teradata", "--database", "db1, ,,, db2 ");

    List<String> databaseNames = arguments.getDatabases();

    assertEquals(ImmutableList.of("db1", "db2"), databaseNames);
  }

  @Test
  public void getHost_notProvidedInFlag_useArgument() {
    ConnectorArguments arguments = arguments("--connector", "oracle");

    assertEquals("default-host-789", arguments.getHost("default-host-789"));
  }

  @Test
  public void getHost_providedInFlag_useFlagAndIgnoreArgument() {
    ConnectorArguments arguments = arguments("--connector", "oracle", "--host", "example-host-123");

    assertEquals("example-host-123", arguments.getHost("default-host-789"));
  }

  @Test
  public void getHostOrDefault_notProvidedInFlag_useDefault() {
    ConnectorArguments arguments = arguments("--connector", "oracle");

    assertEquals(OPT_HOST_DEFAULT, arguments.getHostOrDefault());
  }

  @Test
  public void getHostOrDefault_providedInFlag_useFlag() {
    ConnectorArguments arguments = arguments("--connector", "oracle", "--host", "example-host-123");

    assertEquals("example-host-123", arguments.getHostOrDefault());
  }

  @Test
  public void getHost_notProvidedInFlag_returnNull() {
    ConnectorArguments arguments = arguments("--connector", "oracle");

    assertNull(arguments.getHost());
  }

  @Test
  public void getHost_providedInFlag_useFlag() {
    ConnectorArguments arguments = arguments("--connector", "oracle", "--host", "example-host-123");

    assertEquals("example-host-123", arguments.getHost());
  }

  @Test
  public void getUserOrFail_noUserFlag_throwsException() throws IOException {
    ConnectorArguments arguments = new ConnectorArguments("--connector", "abcABC123");

    Exception exception =
        assertThrows(MetadataDumperUsageException.class, arguments::getUserOrFail);

    assertEquals(
        "Required username was not provided. Please use the '--user' flag to provide the username.",
        exception.getMessage());
  }

  @Test
  public void getUserOrFail_success() throws IOException {
    String expectedName = "admin456";
    ConnectorArguments arguments =
        new ConnectorArguments("--connector", "abcABC123", "--user", expectedName);

    String actualName = arguments.getUserOrFail();

    assertEquals(expectedName, actualName);
  }

  @Test
  public void toString_excludesQueryProperties() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "snowflake-logs",
            "-D",
            "snowflake.logs.query=SELECT * FROM logs",
            "-D",
            "snowflake.logs.where=id = 123",
            "-D",
            "snowflake.warehouse_events_history.query=SELECT * FROM warehouse_events");

    String defaultToString = arguments.toString();
    assertTrue(
        "Should contain query definitions by default",
        defaultToString.contains("SELECT * FROM logs"));
    assertTrue(
        "Should contain query definitions by default",
        defaultToString.contains("SELECT * FROM warehouse_events"));

    String result = arguments.toStringWithoutCustomQueries();

    assertFalse("Should not contain query definitions", result.contains("SELECT * FROM logs"));
    assertFalse(
        "Should not contain query definitions", result.contains("SELECT * FROM warehouse_events"));
    assertTrue("Should contain other definitions", result.contains("id = 123"));
  }

  @Test
  public void getQueryLogEarliestTimestamp_dateOrTimestamp_returnsValue() {
    ImmutableList<String> values =
        ImmutableList.of(
            "2024-03-21",
            "2024-03-21 10:15",
            "2024-03-21 10:15:30",
            "2024-03-21 10:15:30.123456",
            "2024-03-21T10:15",
            "2024-03-21T10:15:30.5");

    for (String value : values) {
      ConnectorArguments arguments =
          arguments(
              "--connector", "snowflake-logs", "--" + OPT_QUERY_LOG_EARLIEST_TIMESTAMP, value);

      assertEquals(value, arguments.getQueryLogEarliestTimestamp());
    }
  }

  @Test
  public void getQueryLogEarliestTimestamp_surroundingSpaces_trimmed() {
    ConnectorArguments arguments =
        arguments(
            "--connector",
            "snowflake-logs",
            "--" + OPT_QUERY_LOG_EARLIEST_TIMESTAMP,
            " 2024-03-21 ");

    assertEquals("2024-03-21", arguments.getQueryLogEarliestTimestamp());
  }

  @Test
  public void getQueryLogEarliestTimestamp_otherValue_throwsUsageException() {
    ImmutableList<String> values =
        ImmutableList.of(
            "CURRENT_TIMESTAMP - INTERVAL '3' DAY",
            "2024-03-21' OR '1'='1",
            "2024-03-21 OR 1=1",
            "2024-03-21 --",
            "2024-13-01",
            "2024-03-21 25:00:00",
            "2024-03-21T10:15:30Z",
            "2024-03-21 10:15:30+01:00",
            "2024-03-21 10",
            "21/03/2024");

    for (String value : values) {
      ConnectorArguments arguments =
          arguments(
              "--connector", "snowflake-logs", "--" + OPT_QUERY_LOG_EARLIEST_TIMESTAMP, value);

      MetadataDumperUsageException e =
          assertThrows(
              value, MetadataDumperUsageException.class, arguments::getQueryLogEarliestTimestamp);
      assertTrue(e.getMessage(), e.getMessage().contains("--" + OPT_QUERY_LOG_START));
    }
  }

  @Test
  public void toString_invalidQueryLogEarliestTimestamp_doesNotThrow() {
    ConnectorArguments arguments =
        arguments("--connector", "snowflake-logs", "--" + OPT_QUERY_LOG_EARLIEST_TIMESTAMP, "now");

    String result = arguments.toString();

    assertTrue(result, result.contains(OPT_QUERY_LOG_EARLIEST_TIMESTAMP + "=now"));
  }

  @Test
  public void isSanitizeCsv_notSpecified_falseAndNotInToString() throws IOException {
    ConnectorArguments arguments = new ConnectorArguments("--connector", "teradata");

    assertFalse(arguments.isSanitizeCsv());
    assertFalse(arguments.toString().contains("sanitize-csv"));
  }

  @Test
  public void isSanitizeCsv_specified_trueAndInToString() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments("--connector", "teradata", "--sanitize-csv");

    assertTrue(arguments.isSanitizeCsv());
    assertTrue(arguments.toString().contains("sanitize-csv=true"));
  }

  // helper method to suppress IOException
  private static ConnectorArguments arguments(String... terms) {
    try {
      return new ConnectorArguments(terms);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
