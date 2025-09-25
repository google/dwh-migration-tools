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

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SnowflakeLiteConnectorTest {

  final SnowflakeLiteConnector connector = new SnowflakeLiteConnector();

  @Test
  public void getDefaultFileName_success() {

    String name = connector.getDefaultFileName(false, null);

    assertTrue(name, name.contains("snowflake"));
    assertTrue(name, name.contains("lite"));
  }

  @Test
  public void getDescription_success() {

    String description = connector.getDescription();

    assertTrue(description, description.contains("lite"));
    assertTrue(description, description.contains("Snowflake"));
  }

  @Test
  public void validate_databaseFlag_throwsException() {
    ImmutableList<String> list =
        ImmutableList.of(
            "--connector", "snowflake-lite", "--assessment", "--database", "SNOWFLAKE");
    ConnectorArguments arguments = ConnectorArguments.create(list);

    assertThrows(SnowflakeUsageException.class, () -> connector.validate(arguments));
  }

  @Test
  public void validate_noAssessmentFlag_throwsUsageException() {
    ImmutableList<String> list = ImmutableList.of("--connector", "snowflake-lite");
    ConnectorArguments arguments = ConnectorArguments.create(list);

    assertThrows(SnowflakeUsageException.class, () -> connector.validate(arguments));
  }

  @Test
  public void validate_correctArguments_noException() {
    ImmutableList<String> list = ImmutableList.of("--connector", "snowflake-lite", "--assessment");
    ConnectorArguments arguments = ConnectorArguments.create(list);

    connector.validate(arguments);
  }
}
