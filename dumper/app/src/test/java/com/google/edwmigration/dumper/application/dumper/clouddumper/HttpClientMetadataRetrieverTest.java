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
package com.google.edwmigration.dumper.application.dumper.clouddumper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class HttpClientMetadataRetrieverTest {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private CloseableHttpClient httpClient;

  private MetadataRetriever underTest;

  @Before
  public void setUp() {
    underTest = new HttpClientMetadataRetriever(httpClient);
  }

  @Test
  public void getMetadata_success() throws Exception {
    ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(HttpStatus.SC_OK);
    httpResponse.setEntity(new StringEntity("Test response"));
    when(httpClient.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation ->
                ((HttpClientResponseHandler) invocation.getArguments()[1])
                    .handleResponse(httpResponse));

    // Act
    Optional<String> response = underTest.get("foo");

    // Verify
    assertEquals(Optional.of("Test response"), response);
  }

  @Test
  public void getMetadata_notFound() throws Exception {
    ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(HttpStatus.SC_NOT_FOUND);
    when(httpClient.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation ->
                ((HttpClientResponseHandler) invocation.getArguments()[1])
                    .handleResponse(httpResponse));

    // Act
    Optional<String> response = underTest.get("foo");

    // Verify
    assertEquals(Optional.empty(), response);
  }

  @Test
  public void getMetadata_failure() throws Exception {
    ClassicHttpResponse httpResponse =
        new BasicClassicHttpResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    httpResponse.setEntity(new StringEntity("Test error"));
    when(httpClient.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation ->
                ((HttpClientResponseHandler) invocation.getArguments()[1])
                    .handleResponse(httpResponse));

    // Act
    HttpException exception = assertThrows(HttpException.class, () -> underTest.get("foo"));

    // Verify
    assertTrue(exception.getMessage().contains("Got unexpected status code 500"));
  }
}
