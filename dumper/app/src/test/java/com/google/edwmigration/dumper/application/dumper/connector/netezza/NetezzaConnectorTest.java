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
package com.google.edwmigration.dumper.application.dumper.connector.netezza;

import static org.junit.Assert.assertThrows;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author shevek */
@RunWith(JUnit4.class)
public class NetezzaConnectorTest extends AbstractConnectorTest {

  private final NetezzaMetadataConnector connector = new NetezzaMetadataConnector();

  @Test
  public void testConnector() throws Exception {
    testConnectorDefaults(connector);
  }

  @Test
  public void validate_regularDatabaseNames_success() throws Exception {
    connector.validate(
        new ConnectorArguments(
            "--connector", "netezza", "--database", "sales,Sales_2024,db$1,_staging,donn\u00e9es"));
  }

  @Test
  public void validate_delimitedDatabaseName_success() throws Exception {
    connector.validate(
        new ConnectorArguments("--connector", "netezza", "--database", "\"Sales Db-2024.v1\""));
  }

  @Test
  public void validate_invalidDatabaseName_throws() throws Exception {
    String[] names = {
      "sales;",
      "sales--",
      "sa'les",
      "sales db",
      "sales.admin",
      "1sales",
      "../sales",
      "sales/x",
      "\"sales\"\"x\"",
      "\"sales/x\"",
      "\"sales\\x\"",
      "\"\"",
      "\"sales"
    };
    for (String name : names) {
      ConnectorArguments arguments =
          new ConnectorArguments("--connector", "netezza", "--database", name);

      assertThrows(name, MetadataDumperUsageException.class, () -> connector.validate(arguments));
    }
  }
}
