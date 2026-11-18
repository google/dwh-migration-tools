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
package com.google.edwmigration.dumper.application.dumper.connector.vertica;

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
public class AbstractVerticaConnectorTest {

  private final AbstractVerticaConnector connector = new VerticaMetadataConnector();

  @Test
  public void open_hostWithQueryDelimiter_throwsUsageException() throws Exception {
    ConnectorArguments arguments = arguments("--host", "localhost?a=b");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(), exception.getMessage().startsWith("The --host value contains '?'"));
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
  public void open_databaseWithWhitespace_throwsUsageException() throws Exception {
    ConnectorArguments arguments = arguments("--database", "db x");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(),
        exception.getMessage().startsWith("The --database value contains the character U+0020"));
  }

  @Test
  public void open_ipv6HostAndName_loadsDriver() throws Exception {
    ConnectorArguments arguments = arguments("--host", "[::1]", "--database", "sales_2024");

    SQLException exception = assertThrows(SQLException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(), exception.getMessage().startsWith("Failed to load any driver"));
  }

  private ConnectorArguments arguments(String... args) throws IOException {
    return new ConnectorArguments(
        ObjectArrays.concat(new String[] {"--connector", connector.getName()}, args, String.class));
  }
}
