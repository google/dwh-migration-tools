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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClouderaConnectorVerifierTest {

  private final CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
  private final ClouderaManagerHandle handle = mock(ClouderaManagerHandle.class);
  private final ConnectorArguments arguments = mock(ConnectorArguments.class);

  @Before
  public void setUp() throws URISyntaxException {
    when(handle.getHttpClient()).thenReturn(httpClient);
    when(handle.getApiURI()).thenReturn(new URI("http://localhost:7183/api/v57"));
  }

  @Test
  public void verify_clusterIsNull_doesNothing() {
    when(arguments.getCluster()).thenReturn(null);

    ClouderaConnectorVerifier.verify(handle, arguments);

    // No exception thrown expected
  }

  @Test
  public void verify_clusterExists_doesNothing() throws Exception {
    when(arguments.getCluster()).thenReturn("cluster-1");
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(httpClient.execute(any())).thenReturn(response);

    ClouderaConnectorVerifier.verify(handle, arguments);

    // No exception thrown expected
  }

  @Test
  public void verify_clusterNotFound_throwsException() throws Exception {
    String clusterName = "cluster-1";
    when(arguments.getCluster()).thenReturn(clusterName);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(statusLine.getStatusCode()).thenReturn(404);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(httpClient.execute(any())).thenReturn(response);

    ClouderaConnectorException e =
        assertThrows(
            ClouderaConnectorException.class,
            () -> ClouderaConnectorVerifier.verify(handle, arguments));

    assertThat(e.getMessage(), containsString("Specified cluster 'cluster-1' not found."));
  }

  @Test
  public void verify_unexpectedApiError_throwsException() throws Exception {
    // Arrange
    String clusterName = "cluster-1";
    when(arguments.getCluster()).thenReturn(clusterName);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(statusLine.getStatusCode()).thenReturn(500);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(response.getEntity()).thenReturn(new StringEntity("Internal Server Error"));
    when(httpClient.execute(any())).thenReturn(response);

    ClouderaConnectorException e =
        assertThrows(
            ClouderaConnectorException.class,
            () -> ClouderaConnectorVerifier.verify(handle, arguments));

    assertThat(
        e.getMessage(),
        containsString(
            "Unexpected API error checking cluster 'cluster-1'. Code: 500. Message: Internal Server Error"));
  }

  @Test
  public void verify_ioException_throwsException() throws Exception {
    String clusterName = "cluster-1";
    when(arguments.getCluster()).thenReturn(clusterName);
    when(httpClient.execute(any())).thenThrow(new IOException("some error"));

    ClouderaConnectorException e =
        assertThrows(
            ClouderaConnectorException.class,
            () -> ClouderaConnectorVerifier.verify(handle, arguments));

    assertThat(
        e.getMessage(),
        containsString(
            "Failed to communicate with Cloudera Manager API while checking cluster 'cluster-1'."));
  }
}
