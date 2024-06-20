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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.util.Properties;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AbstractOracleConnectorTest {
  private static final String EXAMPLE_PASSWORD = "p4ssw0rd";
  private static final String EXAMPLE_USER = "user123";

  private final ConnectorArguments arguments = mock(ConnectorArguments.class);
  private final AbstractOracleConnector connector = new OracleMetadataConnector();

  @Test
  public void buildProperties_success() {
    when(arguments.getPasswordOrPrompt()).thenReturn(EXAMPLE_PASSWORD);
    when(arguments.getUserOrFail()).thenReturn(EXAMPLE_USER);

    Properties actual = connector.buildProperties(arguments);

    assertEquals(EXAMPLE_PASSWORD, actual.getProperty("password"));
    assertEquals(EXAMPLE_USER, actual.getProperty("user"));
  }

  @Test
  public void buildUrl_serviceNameAndSidBothProvided_throwsException() {
    Exception serviceOrSid = OracleConnectorExceptions.mustProvideServiceOrSid();
    when(arguments.getOracleServicename()).thenReturn("ORCLPDB");
    when(arguments.getOracleSID()).thenReturn("ORCLPDB1");

    // Act
    ThrowingRunnable runnable = () -> AbstractOracleConnector.buildUrl(arguments);

    // Assert
    Exception exception = assertThrows(MetadataDumperUsageException.class, runnable);
    assertEquals(serviceOrSid.getMessage(), exception.getMessage());
  }

  @Test
  public void buildUrl_serviceNameAndSidBothNull_throwsException() {
    Exception serviceOrSid = OracleConnectorExceptions.mustProvideServiceOrSid();

    // Act
    ThrowingRunnable runnable = () -> AbstractOracleConnector.buildUrl(arguments);

    // Assert
    Exception exception = assertThrows(MetadataDumperUsageException.class, runnable);
    assertEquals(serviceOrSid.getMessage(), exception.getMessage());
  }

  @Test
  public void buildUrl_providedServiceName_success() {
    when(arguments.getOracleServicename()).thenReturn("ORCLPDB");
    when(arguments.getHost()).thenReturn("localhost");
    when(arguments.getPort(anyInt())).thenReturn(1521);

    // Act
    String url = AbstractOracleConnector.buildUrl(arguments);

    // Assert
    assertEquals("jdbc:oracle:thin:@//localhost:1521/ORCLPDB", url);
  }

  @Test
  public void buildUrl_providedSid_success() {
    when(arguments.getOracleSID()).thenReturn("ORCLPDB1");
    when(arguments.getHost()).thenReturn("localhost");
    when(arguments.getPort(anyInt())).thenReturn(1521);

    // Act
    String url = AbstractOracleConnector.buildUrl(arguments);

    // Assert
    assertEquals("jdbc:oracle:thin:@localhost:1521:ORCLPDB1", url);
  }

  @Test
  public void buildUrl_providedUrl_success() {
    String argumentUrl = "jdbc:oracle:thin:@localhost:1521:ORCLPDB1";
    when(arguments.getUri()).thenReturn(argumentUrl);

    // Act
    String url = AbstractOracleConnector.buildUrl(arguments);

    // Assert
    assertEquals(argumentUrl, url);
  }
}
