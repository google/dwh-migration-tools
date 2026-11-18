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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ObjectArrays;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.io.IOException;
import java.sql.SQLException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AbstractSnowflakeConnectorJdbcUrlTest {

  private final SnowflakeMetadataConnector connector = new SnowflakeMetadataConnector();

  @Test
  public void open_hostWithQueryDelimiter_throwsUsageException() throws Exception {
    ConnectorArguments arguments = arguments("--host", "localhost?a=b");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(), exception.getMessage().startsWith("The --host value contains '?'"));
  }

  @Test
  public void open_warehouseWithParameterDelimiter_throwsUsageException() throws Exception {
    // The empty host makes the driver reject the URL without connecting, if the value got through.
    ConnectorArguments arguments = arguments("--host", "", "--warehouse", "WH&a=b");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(),
        exception.getMessage().startsWith("The --warehouse value contains '&'"));
  }

  @Test
  public void open_roleWithFragment_throwsUsageException() throws Exception {
    ConnectorArguments arguments = arguments("--host", "", "--role", "ANALYST#");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(), exception.getMessage().startsWith("The --role value contains '#'"));
  }

  @Test
  public void open_identifiersAndEncodedRole_reachDriver() throws Exception {
    ConnectorArguments arguments =
        arguments("--host", "", "--warehouse", "COMPUTE_WH$2", "--role", "DATA%20READER");

    SQLException exception = assertThrows(SQLException.class, () -> connector.open(arguments));

    assertTrue(exception.getMessage(), exception.getMessage().contains("Connection string"));
  }

  private ConnectorArguments arguments(String... args) throws IOException {
    return new ConnectorArguments(
        ObjectArrays.concat(
            new String[] {"--connector", connector.getName(), "--user", "testuser"},
            args,
            String.class));
  }
}
