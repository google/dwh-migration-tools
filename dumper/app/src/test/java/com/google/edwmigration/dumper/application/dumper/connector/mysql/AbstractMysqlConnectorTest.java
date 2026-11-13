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
package com.google.edwmigration.dumper.application.dumper.connector.mysql;

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
public class AbstractMysqlConnectorTest {

  private final AbstractMysqlConnector connector = new MysqlMetadataConnector();

  @Test
  public void open_hostWithQueryDelimiter_throwsUsageException() throws Exception {
    ConnectorArguments arguments = arguments("--host", "localhost/db?a=b");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(), exception.getMessage().startsWith("The --host value contains '/'"));
  }

  @Test
  public void open_hostWithFragment_throwsUsageException() throws Exception {
    ConnectorArguments arguments = arguments("--host", "localhost:3307#");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(), exception.getMessage().startsWith("The --host value contains '#'"));
  }

  @Test
  public void open_databaseWithQueryDelimiter_throwsUsageException() throws Exception {
    ConnectorArguments arguments = arguments("--database", "db?a=b");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(),
        exception.getMessage().startsWith("The --database value contains '?'"));
  }

  @Test
  public void open_databaseWithParameterDelimiter_throwsUsageException() throws Exception {
    ConnectorArguments arguments = arguments("--database", "db&a=b");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(),
        exception.getMessage().startsWith("The --database value contains '&'"));
  }

  @Test
  public void open_ipv6HostAndName_loadsDriver() throws Exception {
    ConnectorArguments arguments = arguments("--host", "[::1]", "--database", "sales_2024-v1.x$");

    SQLException exception = assertThrows(SQLException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(), exception.getMessage().startsWith("Failed to load any driver"));
  }

  @Test
  public void open_hostName_loadsDriver() throws Exception {
    ConnectorArguments arguments = arguments("--host", "db-1.example.com", "--port", "3307");

    SQLException exception = assertThrows(SQLException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(), exception.getMessage().startsWith("Failed to load any driver"));
  }

  private ConnectorArguments arguments(String... args) throws IOException {
    return new ConnectorArguments(
        ObjectArrays.concat(new String[] {"--connector", connector.getName()}, args, String.class));
  }
}
