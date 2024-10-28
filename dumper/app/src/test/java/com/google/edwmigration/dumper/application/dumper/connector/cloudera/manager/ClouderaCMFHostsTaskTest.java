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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyChar;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClouderaCMFHostsTaskTest {

  private ClouderaCMFHostsTask task = new ClouderaCMFHostsTask();

  @Mock private TaskRunContext context;
  @Mock private ByteSink sink;
  @Mock private ClouderaManagerHandle handle;
  @Mock private Writer writer;
  @Mock private CharSink charSink;
  @Mock private CloseableHttpClient httpClient;
  private URI uri;

  @Before
  public void setUp() throws Exception {
    uri = URI.create("http://localhost");
    when(handle.getHttpClient()).thenReturn(httpClient);
    when(handle.getBaseURI()).thenReturn(uri);

    when(sink.asCharSink(eq(StandardCharsets.UTF_8))).thenReturn(charSink);
    when(charSink.openBufferedStream()).thenReturn(writer);
  }

  @Test
  public void clusterIdExists_success() throws Exception {
    try (MockedStatic<EntityUtils> mockedUtils = mockStatic(EntityUtils.class)) {
      when(handle.getClusters())
          .thenReturn(
              ImmutableList.of(
                  new ClouderaClusterDTO("id1", "single cluster"),
                  new ClouderaClusterDTO("cld-clst", "cld cluster")));

      CloseableHttpResponse responseId1 = mock(CloseableHttpResponse.class);
      HttpEntity entityId1 = mock(HttpEntity.class);
      when(responseId1.getEntity()).thenReturn(entityId1);

      CloseableHttpResponse responseCldClst = mock(CloseableHttpResponse.class);
      HttpEntity entityCldClst = mock(HttpEntity.class);
      when(responseCldClst.getEntity()).thenReturn(entityCldClst);

      when(httpClient.execute(
              argThat(
                  get -> {
                    return get != null && get.getURI().toString().endsWith("id1");
                  })))
          .thenReturn(responseId1);
      when(httpClient.execute(
              argThat(
                  get -> {
                    return get != null && get.getURI().toString().endsWith("cld-clst");
                  })))
          .thenReturn(responseCldClst);

      mockedUtils
          .when(() -> EntityUtils.toString(entityId1))
          .thenReturn("{'clusterName':'single cluster'}");
      mockedUtils
          .when(() -> EntityUtils.toString(entityCldClst))
          .thenReturn("{\n'clusterName':'cld cluster'\n\r}");

      task.doRun(context, sink, handle);

      Set<URI> requestedUrls = new HashSet<>();
      verify(httpClient, times(2))
          .execute(
              argThat(
                  request -> {
                    assertEquals(HttpGet.class, request.getClass());
                    requestedUrls.add(request.getURI());
                    return true;
                  }));

      assertEquals(
          ImmutableSet.of(
              URI.create("http://localhost/cmf/hardware/hosts/hostsOverview.json?clusterId=id1"),
              URI.create(
                  "http://localhost/cmf/hardware/hosts/hostsOverview.json?clusterId=cld-clst")),
          requestedUrls);

      // write jsonl. https://jsonlines.org/
      Set<String> fileLines = new HashSet<>();
      verify(writer, times(2))
          .write(
              (String)
                  argThat(
                      content -> {
                        String str = (String) content;
                        assertFalse(str.contains("\n"));
                        assertFalse(str.contains("\r"));
                        fileLines.add(str);
                        return true;
                      }));
      verify(writer, times(2)).write('\n');
      assertEquals(
          ImmutableSet.of("{'clusterName':'single cluster'}", "{'clusterName':'cld cluster'}"),
          fileLines);

      verify(responseId1).close();
      verify(responseCldClst).close();
      verify(writer).close();
    }
  }

  @Test
  public void noClusterId_skip_writes() throws Exception {
    when(handle.getClusters())
        .thenReturn(ImmutableList.of(new ClouderaClusterDTO(null, "single cluster")));

    task.doRun(context, sink, handle);

    verify(httpClient, never()).execute(any());

    verifyNoWrites();
  }

  @Test
  public void clustersWereNotInitialized_throwsException() throws Exception {
    when(handle.getClusters()).thenReturn(null);

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    assertEquals(
        "Cloudera clusters must be initialized before hosts dumping.", exception.getMessage());

    verifyNoWrites();
  }

  private void verifyNoWrites() throws IOException {
    verify(writer, never()).write(anyChar());
    verify(writer, never()).write(anyString());
    verify(writer, never()).write(anyString(), anyInt(), anyInt());
    verify(writer, never()).write(any(char[].class));
    verify(writer, never()).write(any(char[].class), anyInt(), anyInt());
  }
}
