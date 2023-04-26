/*
 * Copyright 2022-2023 Google LLC
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

import static org.junit.Assert.assertEquals;
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
  public void getDatabases_success() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments(new String[] {"--connector", "teradata", "--database", "sample-db"});

    List<String> databaseNames = arguments.getDatabases();

    assertEquals(ImmutableList.of("sample-db"), databaseNames);
  }

  @Test
  public void getDatabases_databaseOptionNotSpecified_success() throws IOException {
    ConnectorArguments arguments = new ConnectorArguments(new String[] {"--connector", "teradata"});

    List<String> databaseNames = arguments.getDatabases();

    assertTrue(databaseNames.isEmpty());
  }

  @Test
  public void getDatabases_trimDatabaseNames() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments(new String[] {"--connector", "teradata", "--database", "db1, db2 "});

    List<String> databaseNames = arguments.getDatabases();

    assertEquals(ImmutableList.of("db1", "db2"), databaseNames);
  }

  @Test
  public void getDatabases_trimDatabaseNamesFilteringOutBlankStrings() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments(
            new String[] {"--connector", "teradata", "--database", "db1, ,,, db2 "});

    List<String> databaseNames = arguments.getDatabases();

    assertEquals(ImmutableList.of("db1", "db2"), databaseNames);
  }
}
