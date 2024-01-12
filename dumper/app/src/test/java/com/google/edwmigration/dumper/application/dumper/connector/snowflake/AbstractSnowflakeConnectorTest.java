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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AbstractSnowflakeConnectorTest extends AbstractConnectorTest {
  private static final ImmutableList<? extends String> ARGS =
      ImmutableList.of(
          "--host", "compilerworks.snowflakecomputing.com",
          "--warehouse", "testwh",
          "--user", "testuser",
          "--password", "[redacted]",
          "--role", "tester");

  private final SnowflakeMetadataConnector metadataConnector = new SnowflakeMetadataConnector();

  @Test
  public void openConnection_failsForVeryLongInput() throws IOException {
    List<String> args = new ArrayList<>(ARGS);
    args.add("--connector");
    args.add(metadataConnector.getName());

    args.add("--database");
    args.add(
        "db12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890");

    ConnectorArguments arguments =
        new ConnectorArguments(args.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
    MetadataDumperUsageException e =
        Assert.assertThrows(
            MetadataDumperUsageException.class,
            () -> {
              metadataConnector.open(arguments);
            });
    Assert.assertTrue(e.getMessage().contains("longer than the maximum allowed number"));
  }

  @Test
  public void openConnection_failsForMalformedInput() throws IOException {
    List<String> args = new ArrayList<>(ARGS);
    args.add("--connector");
    args.add(metadataConnector.getName());

    args.add("--database");
    args.add("testdb\";DROP DATABASE testdb");

    ConnectorArguments arguments =
        new ConnectorArguments(args.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
    MetadataDumperUsageException e =
        Assert.assertThrows(
            MetadataDumperUsageException.class,
            () -> {
              metadataConnector.open(arguments);
            });
    Assert.assertTrue(
        e.getMessage().contains("Database name has incorrectly placed double quote(s)."));
  }
}
