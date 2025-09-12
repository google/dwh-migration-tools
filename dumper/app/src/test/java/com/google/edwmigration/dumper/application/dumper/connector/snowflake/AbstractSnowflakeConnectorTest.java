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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorTest;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class AbstractSnowflakeConnectorTest extends AbstractConnectorTest {
  private static final ImmutableList<String> ARGS =
      ImmutableList.of(
          "--host", "compilerworks.snowflakecomputing.com",
          "--warehouse", "testwh",
          "--user", "testuser",
          "--password", "[redacted]",
          "--role", "tester");

  private final SnowflakeMetadataConnector metadataConnector = new SnowflakeMetadataConnector();

  public enum TestCase {
    LOGS(SnowflakeLogsConnector.class),
    LOGS_AU(SnowflakeAccountUsageLogsConnector.class),
    LOGS_IS(SnowflakeInformationSchemaLogsConnector.class),
    META(SnowflakeMetadataConnector.class),
    META_AU(SnowflakeAccountUsageMetadataConnector.class),
    META_IS(SnowflakeInformationSchemaMetadataConnector.class);

    final Class<? extends AbstractSnowflakeConnector> subclass;

    TestCase(Class<? extends AbstractSnowflakeConnector> subclass) {
      this.subclass = subclass;
    }
  }

  @Theory
  public void describeAsDelegate_success(TestCase testCase) throws Exception {
    AbstractSnowflakeConnector connector = testCase.subclass.newInstance();

    String description = AbstractSnowflakeConnector.describeAsDelegate(connector, "test-connector");

    assertTrue(description, description.contains(connector.getName()));
    assertTrue(description, description.contains(connector.getDescription()));
    assertTrue(description, description.contains("[same options as 'test-connector']"));
    assertTrue(description, description.endsWith("\n"));
  }

  @Test
  public void openConnection_failsForVeryLongInput() throws IOException {
    // 262 characters
    String longInput =
        "db12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
    ConnectorArguments arguments =
        makeArguments("--connector", metadataConnector.getName(), "--database", longInput);

    MetadataDumperUsageException e =
        assertThrows(MetadataDumperUsageException.class, () -> metadataConnector.open(arguments));

    assertTrue(e.getMessage(), e.getMessage().contains("longer than the maximum allowed number"));
  }

  @Test
  public void open_malformedInput_fail() throws IOException {
    ConnectorArguments arguments =
        makeArguments(
            "--connector",
            metadataConnector.getName(),
            "--database",
            "testdb\";DROP DATABASE testdb");

    MetadataDumperUsageException e =
        assertThrows(MetadataDumperUsageException.class, () -> metadataConnector.open(arguments));

    assertTrue(
        e.getMessage(),
        e.getMessage().contains("Database name has incorrectly placed double quote(s)."));
  }

  @Test
  public void validate_mixedPrivateKeyAndPassword_fail() throws IOException {
    ConnectorArguments arguments =
        makeArguments(
            "--connector", metadataConnector.getName(), "--private-key-file", "/path/to/file.r8");

    MetadataDumperUsageException e =
        assertThrows(
            MetadataDumperUsageException.class, () -> metadataConnector.validate(arguments));

    assertTrue(
        e.getMessage(),
        e.getMessage()
            .contains(
                "Private key authentication method can't be used together with user password"));
  }

  enum TestEnum {
    SomeValue
  }

  @Test
  public void columnOf_success() {

    String columnName = AbstractSnowflakeConnector.columnOf(TestEnum.SomeValue);

    assertEquals("SOME_VALUE", columnName);
  }

  @Test
  public void validate_assessmentEnabledWithDatabaseFilter_throwsUsageException()
      throws IOException {
    ConnectorArguments arguments =
        makeArguments("--connector", "snowflake", "--database", "SNOWFLAKE", "--assessment");

    MetadataDumperUsageException e =
        assertThrows(
            MetadataDumperUsageException.class, () -> metadataConnector.validate(arguments));

    assertTrue(
        e.getMessage(),
        e.getMessage().contains("Trying to filter by database with the --assessment flag."));
  }

  @Test
  public void checkJnaInClasspath_success() {
    try {
      // JNA is required for the Snowflake MFA caching mechanism
      Class.forName("com.sun.jna.Library");
    } catch (ClassNotFoundException e) {
      Assert.fail(
          "net.java.dev.jna was not found in the classpath it is required for the Snowflake MFA caching.");
    }
  }

  private static ConnectorArguments makeArguments(String... extraArguments) {
    ArrayList<String> arguments = new ArrayList<>(ARGS);
    for (String item : extraArguments) {
      arguments.add(item);
    }
    return ConnectorArguments.create(arguments);
  }
}
