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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import java.util.Properties;
import org.junit.Test;
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
}
