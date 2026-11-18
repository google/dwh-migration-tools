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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.core.DatabricksConfig;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JdbcSqlRunnerTest {

  private static final String CLUSTER_ID = "0123-456789-abcdefgh";
  private static final String WAREHOUSE_ID = "abcdef0123456789";
  private static final String WORKSPACE_ID = "1234567890123456";
  private static final String URL_OPTIONS =
      ";AuthMech=3;EnableTelemetry=0;UseThriftClient=1;EnableQueryResultDownload=0";

  @Test
  public void createClusterUrl_returnsThriftEndpointOfCluster() throws IOException {
    DatabricksConfig config =
        new DatabricksConfig()
            .setHost("https://adb-1234567890123456.7.azuredatabricks.net")
            .setWorkspaceId(WORKSPACE_ID);

    assertEquals(
        "jdbc:databricks://adb-1234567890123456.7.azuredatabricks.net:443/default;"
            + "transportMode=http;ssl=1;httpPath=sql/protocolv1/o/1234567890123456/"
            + "0123-456789-abcdefgh"
            + URL_OPTIONS,
        JdbcSqlRunner.createClusterUrl(config, CLUSTER_ID));
  }

  @Test
  public void createClusterUrl_withoutWorkspaceId_throwsWithEnvironmentVariable() {
    DatabricksConfig config =
        new DatabricksConfig().setHost("https://example.cloud.databricks.com");

    IOException thrown =
        assertThrows(IOException.class, () -> JdbcSqlRunner.createClusterUrl(config, CLUSTER_ID));

    assertTrue(thrown.getMessage(), thrown.getMessage().contains("DATABRICKS_WORKSPACE_ID"));
  }

  @Test
  public void createWarehouseUrl_returnsThriftEndpointOfWarehouseWithQueryTag() throws IOException {
    DatabricksConfig config =
        new DatabricksConfig().setHost("https://example.cloud.databricks.com/");

    assertEquals(
        "jdbc:databricks://example.cloud.databricks.com:443/default;transportMode=http;ssl=1;"
            + "httpPath=sql/1.0/warehouses/abcdef0123456789"
            + URL_OPTIONS
            + ";query_tags=source:dwh-migration-dumper",
        JdbcSqlRunner.createWarehouseUrl(config, WAREHOUSE_ID));
  }

  @Test
  public void createClusterUrl_clusterIdWithDelimiter_throwsUsageException() {
    DatabricksConfig config =
        new DatabricksConfig()
            .setHost("https://example.cloud.databricks.com")
            .setWorkspaceId(WORKSPACE_ID);

    MetadataDumperUsageException thrown =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> JdbcSqlRunner.createClusterUrl(config, CLUSTER_ID + ";a=b"));

    assertTrue(
        thrown.getMessage(), thrown.getMessage().startsWith("The --cluster value contains ';'"));
  }

  @Test
  public void createWarehouseUrl_warehouseIdWithDelimiter_throwsUsageException() {
    DatabricksConfig config =
        new DatabricksConfig().setHost("https://example.cloud.databricks.com/");

    MetadataDumperUsageException thrown =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> JdbcSqlRunner.createWarehouseUrl(config, WAREHOUSE_ID + ";a=b"));

    assertTrue(
        thrown.getMessage(), thrown.getMessage().startsWith("The --warehouse value contains ';'"));
  }

  @Test
  public void createWarehouseUrl_withoutHost_throws() {
    DatabricksConfig config = new DatabricksConfig().setHost("example.cloud.databricks.com");

    assertThrows(IOException.class, () -> JdbcSqlRunner.createWarehouseUrl(config, WAREHOUSE_ID));
  }

  @Test
  public void formatUrl_usesSchemeAndPortOfHost() throws URISyntaxException {
    assertEquals(
        "jdbc:databricks://localhost:80/default;transportMode=http;ssl=0;httpPath=sql/path"
            + URL_OPTIONS,
        JdbcSqlRunner.formatUrl(new URI("http://localhost"), "sql/path"));
    assertEquals(
        "jdbc:databricks://localhost:8080/default;transportMode=http;ssl=0;httpPath=sql/path"
            + URL_OPTIONS,
        JdbcSqlRunner.formatUrl(new URI("http://localhost:8080"), "sql/path"));
    assertEquals(
        "jdbc:databricks://localhost:8443/default;transportMode=http;ssl=1;httpPath=sql/path"
            + URL_OPTIONS,
        JdbcSqlRunner.formatUrl(new URI("https://localhost:8443"), "sql/path"));
  }
}
