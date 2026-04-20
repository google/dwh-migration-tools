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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaYarnApplicationDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClouderaSparkYarnApplicationMetadataTaskTest {

  private static WireMockServer server;
  private ClouderaSparkYarnApplicationMetadataTask task;
  private ClouderaManagerHandle handle;

  @Mock private TaskRunContext context;
  @Mock private ConnectorArguments arguments;

  private final ByteArrayOutputStream bos = new ByteArrayOutputStream();
  private final ByteSink sink =
      new ByteSink() {
        @Override
        public OutputStream openStream() {
          return bos;
        }
      };

  @BeforeClass
  public static void beforeClass() throws Exception {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort().httpsPort(0));
    server.start();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    server.stop();
  }

  @Before
  public void setUp() throws Exception {
    server.resetAll();
    task = new ClouderaSparkYarnApplicationMetadataTask(TaskCategory.OPTIONAL);
    URI uri = URI.create("https://localhost:" + server.httpsPort() + "/api/v40/");
    CloseableHttpClient trustAllClient = createTrustAllClient();
    handle = new ClouderaManagerHandle(uri, trustAllClient, trustAllClient);
    org.mockito.Mockito.when(context.getArguments()).thenReturn(arguments);
    org.mockito.Mockito.when(arguments.getSparkHistoryServiceNames())
        .thenReturn(ImmutableList.of());
  }

  private CloseableHttpClient createTrustAllClient() throws Exception {
    HttpClientBuilder builder = HttpClients.custom();
    builder.setSSLContext(
        new SSLContextBuilder().loadTrustMaterial(null, TrustAllStrategy.INSTANCE).build());
    builder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
    return builder.build();
  }

  @Test
  public void doRun_success() throws Exception {
    // Arrange
    String clusterName = "cluster1";
    handle.initClusters(ImmutableList.of(ClouderaClusterDTO.create("c1", clusterName)));
    handle.initSparkYarnApplications(
        ImmutableList.of(ClouderaYarnApplicationDTO.create("app1", clusterName)));

    String wireMockHost = "localhost:" + server.httpsPort();
    stubKnoxDiscovery(clusterName, wireMockHost);

    server.stubFor(
        get(urlEqualTo("/cluster1/cdp-proxy-api/spark3history/api/v1/applications?limit=1"))
            .willReturn(aResponse().withStatus(200).withBody("[]")));

    byte[] zipContent =
        Resources.toByteArray(Resources.getResource("cloudera/manager/spark-event-log.zip"));
    server.stubFor(
        get(urlEqualTo("/cluster1/cdp-proxy-api/spark3history/api/v1/applications/app1/logs"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/zip")
                    .withBody(zipContent)));

    // Act
    task.doRun(context, sink, handle);

    // Assert
    String output = bos.toString(StandardCharsets.UTF_8.name());
    assertTrue(
        "Output should contain application ID", output.contains("\"applicationId\":\"app1\""));
    assertTrue(
        "Output should contain cluster name", output.contains("\"clusterName\":\"cluster1\""));
    assertTrue(
        "Output should contain spark version", output.contains("\"sparkVersion\":\"3.1.1\""));
    assertTrue(
        "Output should contain application type",
        output.contains("\"sparkApplicationType\":\"SparkSQL\""));
  }

  @Test
  public void doRun_multipleApplications_success() throws Exception {
    // Arrange
    String clusterName = "cluster1";
    handle.initClusters(ImmutableList.of(ClouderaClusterDTO.create("c1", clusterName)));
    handle.initSparkYarnApplications(
        ImmutableList.of(
            ClouderaYarnApplicationDTO.create("app1", clusterName),
            ClouderaYarnApplicationDTO.create("app2", clusterName)));

    String wireMockHost = "localhost:" + server.httpsPort();
    stubKnoxDiscovery(clusterName, wireMockHost);

    server.stubFor(
        get(urlEqualTo("/cluster1/cdp-proxy-api/spark3history/api/v1/applications?limit=1"))
            .willReturn(aResponse().withStatus(200).withBody("[]")));

    byte[] zipContent =
        Resources.toByteArray(Resources.getResource("cloudera/manager/spark-event-log.zip"));

    server.stubFor(
        get(urlEqualTo("/cluster1/cdp-proxy-api/spark3history/api/v1/applications/app1/logs"))
            .willReturn(aResponse().withStatus(200).withBody(zipContent)));

    server.stubFor(
        get(urlEqualTo("/cluster1/cdp-proxy-api/spark3history/api/v1/applications/app2/logs"))
            .willReturn(aResponse().withStatus(200).withBody(zipContent)));

    // Act
    task.doRun(context, sink, handle);

    // Assert
    String output = bos.toString(StandardCharsets.UTF_8.name());
    assertTrue(output.contains("\"applicationId\":\"app1\""));
    assertTrue(output.contains("\"clusterName\":\"cluster1\""));
    assertTrue(output.contains("\"applicationId\":\"app2\""));
  }

  @Test
  public void doRun_multipleClusters_success() throws Exception {
    // Arrange
    handle.initClusters(
        ImmutableList.of(
            ClouderaClusterDTO.create("c1", "cluster1"),
            ClouderaClusterDTO.create("c2", "cluster2")));
    handle.initSparkYarnApplications(
        ImmutableList.of(
            ClouderaYarnApplicationDTO.create("app1", "cluster1"),
            ClouderaYarnApplicationDTO.create("app2", "cluster2")));

    String wireMockHost = "localhost:" + server.httpsPort();
    stubKnoxDiscovery("cluster1", wireMockHost);
    stubKnoxDiscovery("cluster2", wireMockHost);

    // Cluster 1 History Server
    server.stubFor(
        get(urlEqualTo("/cluster1/cdp-proxy-api/spark3history/api/v1/applications?limit=1"))
            .willReturn(aResponse().withStatus(200).withBody("[]")));

    // Cluster 2 History Server
    server.stubFor(
        get(urlEqualTo("/cluster2/cdp-proxy-api/spark3history/api/v1/applications?limit=1"))
            .willReturn(aResponse().withStatus(200).withBody("[]")));

    byte[] zipContent =
        Resources.toByteArray(Resources.getResource("cloudera/manager/spark-event-log.zip"));

    server.stubFor(
        get(urlEqualTo("/cluster1/cdp-proxy-api/spark3history/api/v1/applications/app1/logs"))
            .willReturn(aResponse().withStatus(200).withBody(zipContent)));

    server.stubFor(
        get(urlEqualTo("/cluster2/cdp-proxy-api/spark3history/api/v1/applications/app2/logs"))
            .willReturn(aResponse().withStatus(200).withBody(zipContent)));

    // Act
    task.doRun(context, sink, handle);

    // Assert
    String output = bos.toString(StandardCharsets.UTF_8.name());
    assertTrue(output.contains("\"applicationId\":\"app1\""));
    assertTrue(output.contains("\"clusterName\":\"cluster1\""));
    assertTrue(output.contains("\"applicationId\":\"app2\""));
    assertTrue(output.contains("\"clusterName\":\"cluster2\""));
  }

  @Test
  public void doRun_historyServerNotFound_writesNothing() throws Exception {
    // Arrange
    String clusterName = "cluster1";
    handle.initClusters(ImmutableList.of(ClouderaClusterDTO.create("c1", clusterName)));
    handle.initSparkYarnApplications(
        ImmutableList.of(ClouderaYarnApplicationDTO.create("app1", clusterName)));

    String wireMockHost = "localhost:" + server.httpsPort();
    stubKnoxDiscovery(clusterName, wireMockHost);

    server.stubFor(
        get(urlEqualTo("/cluster1/cdp-proxy-api/spark3history/api/v1/applications?limit=1"))
            .willReturn(aResponse().withStatus(404)));
    server.stubFor(
        get(urlEqualTo("/cluster1/cdp-proxy-api/sparkhistory/api/v1/applications?limit=1"))
            .willReturn(aResponse().withStatus(404)));

    // Act
    task.doRun(context, sink, handle);

    // Assert
    assertEquals("", bos.toString());
  }

  @Test
  public void doRun_nullClusters_throwsException() {
    // Arrange (handle clusters not initialized)

    // Act & Assert
    assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));
  }

  @Test
  public void doRun_nullApplications_throwsException() {
    // Arrange
    handle.initClusters(ImmutableList.of(ClouderaClusterDTO.create("c1", "cluster1")));
    // sparkYarnApplications not initialized

    // Act & Assert
    assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));
  }

  private void stubKnoxDiscovery(String clusterName, String hostname) {
    server.stubFor(
        get(urlEqualTo("/api/v40/clusters/" + clusterName + "/services"))
            .willReturn(okJson("{\"items\":[{\"name\":\"knox-service\",\"type\":\"KNOX\"}]}")));

    server.stubFor(
        get(urlEqualTo("/api/v40/clusters/" + clusterName + "/services/knox-service/roles"))
            .willReturn(
                okJson(
                    "{\"items\":[{\"hostRef\":{\"hostname\":\""
                        + hostname
                        + "\"},\"roleConfigGroupRef\":{\"roleConfigGroupName\":\"knox-RCG\"}}]}")));

    server.stubFor(
        get(urlEqualTo(
                "/api/v40/clusters/"
                    + clusterName
                    + "/services/knox-service/roleConfigGroups/knox-RCG/config"))
            .willReturn(
                okJson(
                    "{\"items\":[{\"name\":\"gateway_path\",\"value\":\""
                        + clusterName
                        + "\"},{\"name\":\"gateway_default_api_topology_name\",\"value\":\"cdp-proxy-api\"}]}")));
  }
}
