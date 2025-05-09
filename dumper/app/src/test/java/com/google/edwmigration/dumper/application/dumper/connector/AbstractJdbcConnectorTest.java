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
package com.google.edwmigration.dumper.application.dumper.connector;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ServiceLoader;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author ishmum */
@RunWith(JUnit4.class)
public class AbstractJdbcConnectorTest extends AbstractConnectorTest {

  private final ServiceLoader<AbstractJdbcConnector> connectors =
      ServiceLoader.load(AbstractJdbcConnector.class);

  @Test
  public void testFailsForInvalidQueryLogTimespan() throws IOException {
    for (AbstractJdbcConnector connector : connectors) {
      ConnectorArguments arguments =
          new ConnectorArguments("--query-log-days", "0", "--connector", connector.getName());

      MetadataDumperUsageException exception =
          Assert.assertThrows(
              "No exception thrown from " + connector.getName(),
              MetadataDumperUsageException.class,
              () -> connector.addTasksTo(new ArrayList<>(), arguments));
      Assert.assertTrue(
          exception.getMessage().startsWith("At least one day of query logs should be exported"));
    }
  }
}
