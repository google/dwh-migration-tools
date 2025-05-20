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
package com.google.edwmigration.dumper.application.dumper.clouddumper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import com.google.edwmigration.dumper.application.dumper.clouddumper.DriverRetriever.DriverInformationMapBuilder;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class DriverRetrieverTest {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private CloseableHttpClient httpClient;

  private Path driverOutputPath;
  private DriverRetriever underTest;

  @Before
  public void setUp() throws Exception {
    driverOutputPath = Files.createTempDirectory("driver-retriever-test");
    underTest =
        new DriverRetriever(
            httpClient,
            driverOutputPath,
            new DriverInformationMapBuilder()
                .addDriver("test", new URI("http://test.google.com/my/driver.jar"), "test_alias")
                .addDriver(
                    "test_with_checksum",
                    new URI("http://test.google.com/my/checked_driver.jar"),
                    BaseEncoding.base16()
                        .lowerCase()
                        .decode("202d40302f856f7f6ec75335254169c600549427e13d712de10f8029854ca99a"))
                .build());
  }

  @Test
  public void getDriver_success() throws Exception {
    ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(HttpStatus.SC_OK);
    httpResponse.setEntity(new StringEntity("Test driver"));
    when(httpClient.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation ->
                ((HttpClientResponseHandler) invocation.getArguments()[1])
                    .handleResponse(httpResponse));

    // Act
    Optional<Path> driverPath = underTest.getDriver("test");

    // Verify
    assertEquals(Optional.of(driverOutputPath.resolve("driver.jar")), driverPath);
    assertEquals(
        ImmutableList.of("Test driver"), Files.readAllLines(driverPath.get(), Charsets.UTF_8));
    ArgumentCaptor<ClassicHttpRequest> requestCaptor =
        ArgumentCaptor.forClass(ClassicHttpRequest.class);
    verify(httpClient).execute(requestCaptor.capture(), any(HttpClientResponseHandler.class));
    assertEquals(
        new URI("http://test.google.com/my/driver.jar"), requestCaptor.getValue().getUri());
  }

  @Test
  public void getDriver_aliasSuccess() throws Exception {
    ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(HttpStatus.SC_OK);
    httpResponse.setEntity(new StringEntity("Test driver"));
    when(httpClient.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation ->
                ((HttpClientResponseHandler) invocation.getArguments()[1])
                    .handleResponse(httpResponse));

    // Act
    Optional<Path> driverPath = underTest.getDriver("test_alias");

    // Verify
    assertEquals(Optional.of(driverOutputPath.resolve("driver.jar")), driverPath);
  }

  @Test
  public void getDriver_emptyForUnknownConnector() throws Exception {
    // Act
    Optional<Path> driverPath = underTest.getDriver("nulldb");

    // Verify
    assertEquals(Optional.empty(), driverPath);
  }

  @Test
  public void getDriver_successWithCorrecChecksum() throws Exception {
    ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(HttpStatus.SC_OK);
    httpResponse.setEntity(new StringEntity("Checked test driver"));
    when(httpClient.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation ->
                ((HttpClientResponseHandler) invocation.getArguments()[1])
                    .handleResponse(httpResponse));

    // Act
    Optional<Path> driverPath = underTest.getDriver("test_with_checksum");

    // Verify
    assertEquals(Optional.of(driverOutputPath.resolve("checked_driver.jar")), driverPath);
    assertEquals(
        ImmutableList.of("Checked test driver"),
        Files.readAllLines(driverPath.get(), Charsets.UTF_8));
  }

  @Test
  public void getDriver_throwsOnFailureToRetrieve() throws Exception {
    ClassicHttpResponse httpResponse =
        new BasicClassicHttpResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(httpClient.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation ->
                ((HttpClientResponseHandler) invocation.getArguments()[1])
                    .handleResponse(httpResponse));

    // Act
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> underTest.getDriver("test"));

    // Verify
    assertTrue(
        "Actual message: " + exception.getMessage(),
        exception
            .getMessage()
            .contains("Failed to get driver for 'test': Got unexpected response code 500 for URI"));
  }

  @Test
  public void getDriver_throwsOnChecksumMismatch() throws Exception {
    ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(HttpStatus.SC_OK);
    httpResponse.setEntity(new StringEntity("Corrupt test driver"));
    when(httpClient.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation ->
                ((HttpClientResponseHandler) invocation.getArguments()[1])
                    .handleResponse(httpResponse));

    // Act
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> underTest.getDriver("test_with_checksum"));

    // Verify
    assertTrue(
        "Actual message: " + exception.getMessage(),
        exception
            .getMessage()
            .contains(
                "Retrieved driver for test_with_checksum expected to have SHA256 sum "
                    + "'202d40302f856f7f6ec75335254169c600549427e13d712de10f8029854ca99a' but got"));
  }
}
