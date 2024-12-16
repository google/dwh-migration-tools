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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiClusterListDto;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.http.HttpEntity;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClouderaClustersTaskTest {

  private final ClouderaClustersTask task = new ClouderaClustersTask();

  private final ObjectMapper objectMapper = new ObjectMapper();
  private ClouderaManagerHandle handle;
  @Mock private TaskRunContext context;
  @Mock private ConnectorArguments arguments;
  @Mock private ByteSink sink;

  @Mock private Writer writer;
  @Mock private CharSink charSink;
  @Mock private CloseableHttpClient httpClient;

  private URI apiUri;

  private String apiClusterListJson;
  private String apiClusterJson;
  private String clusterStatusJson;
  private final int clusterStatusId = 1546336862;

  @Before
  public void setUp() throws Exception {
    apiUri = URI.create("http://localhost/api");
    handle = new ClouderaManagerHandle(apiUri, httpClient);

    when(sink.asCharSink(eq(StandardCharsets.UTF_8))).thenReturn(charSink);
    when(charSink.openBufferedStream()).thenReturn(writer);
    when(context.getArguments()).thenReturn(arguments);

    apiClusterListJson = readString("/cloudera/manager/dto/ApiClusterList.json");
    apiClusterJson = readString("/cloudera/manager/dto/ApiCluster.json");
    clusterStatusJson = readString("/cloudera/manager/cluster-status.json");
  }

  @Test
  public void clusterNotProvided_success() throws Exception {
    when(arguments.getCluster()).thenReturn(null);

    CloseableHttpResponse clustersResponse = mock(CloseableHttpResponse.class);
    HttpEntity clustersEntity = mock(HttpEntity.class);
    when(clustersResponse.getEntity()).thenReturn(clustersEntity);
    when(httpClient.execute(
            argThat(get -> get != null && get.getURI().toString().endsWith("/clusters"))))
        .thenReturn(clustersResponse);
    when(clustersEntity.getContent())
        .thenReturn(new ByteArrayInputStream(apiClusterListJson.getBytes()));

    // request for cluster aaa
    CloseableHttpResponse statusAResponse = mock(CloseableHttpResponse.class);
    HttpEntity statusAEntity = mock(HttpEntity.class);
    when(statusAResponse.getEntity()).thenReturn(statusAEntity);
    when(httpClient.execute(
            argThat(get -> get != null && get.getURI().toString().endsWith("/aaa/status.json"))))
        .thenReturn(statusAResponse);
    when(statusAEntity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                clusterStatusJson.replaceAll("" + clusterStatusId, "111").getBytes()));
    when(statusAResponse.getStatusLine())
        .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "Ok"));

    // request for cluster bbb
    CloseableHttpResponse statusBResponse = mock(CloseableHttpResponse.class);
    HttpEntity statusBEntity = mock(HttpEntity.class);
    when(statusBResponse.getEntity()).thenReturn(statusBEntity);
    when(httpClient.execute(
            argThat(get -> get != null && get.getURI().toString().endsWith("/bbb/status.json"))))
        .thenReturn(statusBResponse);
    when(statusBEntity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                clusterStatusJson.replaceAll("" + clusterStatusId, "222").getBytes()));
    when(statusBResponse.getStatusLine())
        .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 401, "not found"));

    task.doRun(context, sink, handle);

    verify(writer)
        .write(
            (String)
                argThat(
                    content -> {
                      try {
                        ApiClusterListDto listDto =
                            objectMapper.readValue((String) content, ApiClusterListDto.class);
                        assertNotNull(listDto.getClusters());
                      } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                      }
                      return true;
                    }));

    verify(httpClient)
        .execute(
            argThat(
                request -> {
                  assertEquals(HttpGet.class, request.getClass());

                  return URI.create("http://localhost/api/clusters").equals(request.getURI());
                }));
    verify(httpClient)
        .execute(
            argThat(
                request -> {
                  assertEquals(HttpGet.class, request.getClass());

                  return URI.create("http://localhost//cmf/clusters/aaa/status.json")
                      .equals(request.getURI());
                }));
    verify(httpClient)
        .execute(
            argThat(
                request -> {
                  assertEquals(HttpGet.class, request.getClass());

                  return URI.create("http://localhost//cmf/clusters/bbb/status.json")
                      .equals(request.getURI());
                }));

    ImmutableList<ClouderaClusterDTO> clusters = handle.getClusters();
    assertNotNull(clusters);
    assertEquals(
        ImmutableList.of(
            ClouderaClusterDTO.create("111", "aaa"), ClouderaClusterDTO.create(null, "bbb")),
        clusters);

    verify(clustersResponse).close();
    verify(writer).close();

    verify(statusAResponse).close();
    verify(statusBResponse).close();
  }

  @Test
  public void clusterProvided_success() throws Exception {
    when(arguments.getCluster()).thenReturn("my-cluster");

    CloseableHttpResponse clustersResponse = mock(CloseableHttpResponse.class);
    HttpEntity clustersEntity = mock(HttpEntity.class);
    when(clustersResponse.getEntity()).thenReturn(clustersEntity);
    when(httpClient.execute(
            argThat(
                get -> get != null && get.getURI().toString().endsWith("/clusters/my-cluster"))))
        .thenReturn(clustersResponse);
    when(clustersEntity.getContent())
        .thenReturn(new ByteArrayInputStream(apiClusterJson.getBytes()));

    // request for cluster my-Cluster
    CloseableHttpResponse statusResponse = mock(CloseableHttpResponse.class);
    HttpEntity statusEntity = mock(HttpEntity.class);
    when(statusResponse.getEntity()).thenReturn(statusEntity);
    when(httpClient.execute(
            argThat(
                get -> get != null && get.getURI().toString().endsWith("/my-cluster/status.json"))))
        .thenReturn(statusResponse);
    when(statusEntity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                clusterStatusJson.replaceAll("" + clusterStatusId, "123").getBytes()));
    when(statusResponse.getStatusLine())
        .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "Ok"));

    task.doRun(context, sink, handle);

    verify(writer)
        .write(
            (String)
                argThat(
                    content -> {
                      try {
                        ApiClusterListDto listDto =
                            objectMapper.readValue((String) content, ApiClusterListDto.class);
                        assertNotNull(listDto.getClusters());
                      } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                      }
                      return true;
                    }));

    verify(httpClient)
        .execute(
            argThat(
                request -> {
                  assertEquals(HttpGet.class, request.getClass());

                  return URI.create("http://localhost/api/clusters/my-cluster")
                      .equals(request.getURI());
                }));
    verify(httpClient)
        .execute(
            argThat(
                request -> {
                  assertEquals(HttpGet.class, request.getClass());

                  return URI.create("http://localhost//cmf/clusters/my-cluster/status.json")
                      .equals(request.getURI());
                }));

    ImmutableList<ClouderaClusterDTO> clusters = handle.getClusters();
    assertNotNull(clusters);
    assertEquals(ImmutableList.of(ClouderaClusterDTO.create("123", "my-cluster")), clusters);

    verify(clustersResponse).close();
    verify(writer).close();

    verify(statusResponse).close();
  }

  private String readString(String name) throws IOException, URISyntaxException {
    return new String(Files.readAllBytes(Paths.get(this.getClass().getResource(name).toURI())));
  }
}
