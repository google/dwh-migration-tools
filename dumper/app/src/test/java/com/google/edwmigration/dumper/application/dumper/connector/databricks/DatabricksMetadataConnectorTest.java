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
package com.google.edwmigration.dumper.application.dumper.connector.databricks;

import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksMetadataConnectorTest {

  private static final String CLUSTER_ID = "0123-456789-abcdefgh";
  private static final String TRANSPORT_PROPERTY = "-Ddatabricks.metadata.warehouse-transport=";

  private final DatabricksMetadataConnector connector = new DatabricksMetadataConnector();

  @Test
  public void validate_checksComputeAndTransportArguments() {
    connector.validate(arguments("--database", "hive_metastore", "--cluster", CLUSTER_ID));
    connector.validate(arguments("--warehouse", " wh-1 ", TRANSPORT_PROPERTY + " INLINE "));

    assertValidateRejects("--warehouse", "wh-1", "--cluster", CLUSTER_ID);
    assertValidateRejects("--database", "hive_metastore");
    assertValidateRejects("--database", "hive_metastore", "--warehouse", " ");
    assertValidateRejects(TRANSPORT_PROPERTY + "inline");
    assertValidateRejects("--warehouse", "wh-1", TRANSPORT_PROPERTY + "odbc");
    assertValidateRejects("--cluster", CLUSTER_ID, "--thread-pool-size", "0");
  }

  private void assertValidateRejects(String... flags) {
    assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments(flags)));
  }

  private static ConnectorArguments arguments(String... arguments) {
    return ConnectorArguments.create(
        ImmutableList.<String>builder()
            .add("--connector", DatabricksMetadataConnector.CONNECTOR_NAME)
            .add("--url", "https://example.cloud.databricks.com")
            .add(arguments)
            .build());
  }
}
