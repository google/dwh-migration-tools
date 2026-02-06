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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.exception.SparkHistoryConnectionException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.exception.SparkLogFormatException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkYarnApplicationMetadata;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SparkJobMetadataExtractorTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private CloseableHttpClient httpClient;
  private SparkJobMetadataExtractor extractor;

  @Before
  public void setUp() {
    httpClient = mock(CloseableHttpClient.class);
    extractor = new SparkJobMetadataExtractor(mapper, httpClient);
  }

  @Test
  public void extract_notFoundStatus_returnsEmpty() throws Exception {
    // Arrange
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(statusLine.getStatusCode()).thenReturn(404);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(httpClient.execute(any(HttpGet.class))).thenReturn(response);

    // Act
    Optional<SparkYarnApplicationMetadata> result =
        extractor.extract("http://log-url", "test-cluster", "app-id");

    // Assert
    assertFalse(result.isPresent());
  }

  @Test
  public void extract_errorStatus_throwsException() throws Exception {
    // Arrange
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(statusLine.getStatusCode()).thenReturn(503);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(httpClient.execute(any(HttpGet.class))).thenReturn(response);

    // Act
    assertThrows(
        SparkHistoryConnectionException.class,
        () -> extractor.extract("http://log-url", "test-cluster", "app-id"));
  }

  @Test
  public void extract_emptyContent_returnsEmpty() throws Exception {
    // Arrange
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(response.getStatusLine()).thenReturn(statusLine);

    HttpEntity entity = mock(HttpEntity.class);
    when(entity.getContentLength()).thenReturn(0L);
    when(response.getEntity()).thenReturn(entity);

    when(httpClient.execute(any(HttpGet.class))).thenReturn(response);

    // Act
    Optional<SparkYarnApplicationMetadata> result =
        extractor.extract("http://log-url", "test-cluster", "app-id");

    // Assert
    assertFalse(result.isPresent());
  }

  @Test
  public void extract_invalidZip_throwsException() throws Exception {
    // Arrange
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(response.getStatusLine()).thenReturn(statusLine);

    HttpEntity entity = mock(HttpEntity.class);
    when(entity.getContentLength()).thenReturn(100L);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream("not a zip".getBytes()));
    when(response.getEntity()).thenReturn(entity);

    when(httpClient.execute(any(HttpGet.class))).thenReturn(response);

    // Act
    assertThrows(
        SparkLogFormatException.class,
        () -> extractor.extract("http://log-url", "test-cluster", "app-id"));
  }

  @Test
  public void extract_validLog_returnsMetadata() throws Exception {
    // Arrange
    String logContent =
        "{\"Event\": \"SparkListenerLogStart\", \"Spark Version\": \"3.1.1\"}\n"
            + "{\"Event\": \"SparkListenerEnvironmentUpdate\", \"Spark Properties\": {\"spark.app.name\": \"SparkSQL\", \"sun.java.command\": \"SparkSQLCLIDriver\"}}\n";
    byte[] zipData = createZipWithLog(logContent);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(response.getStatusLine()).thenReturn(statusLine);

    HttpEntity entity = mock(HttpEntity.class);
    when(entity.getContentLength()).thenReturn((long) zipData.length);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream(zipData));
    when(response.getEntity()).thenReturn(entity);

    when(httpClient.execute(any(HttpGet.class))).thenReturn(response);

    // Act
    Optional<SparkYarnApplicationMetadata> result =
        extractor.extract("http://log-url", "test-cluster", "app-id");

    // Assert
    assertTrue(result.isPresent());
    assertEquals("test-cluster", result.get().getClusterName());
    assertEquals("app-id", result.get().getApplicationId());
    assertEquals("3.1.1", result.get().getSparkVersion());
    assertEquals("SparkSQL", result.get().getSparkApplicationType());
  }

  @Test
  public void extract_partialLog_returnsMetadata() throws Exception {
    // Arrange
    String logContent = "{\"Event\": \"SparkListenerLogStart\", \"Spark Version\": \"2.4.5\"}\n";
    byte[] zipData = createZipWithLog(logContent);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(response.getStatusLine()).thenReturn(statusLine);

    HttpEntity entity = mock(HttpEntity.class);
    when(entity.getContentLength()).thenReturn((long) zipData.length);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream(zipData));
    when(response.getEntity()).thenReturn(entity);

    when(httpClient.execute(any(HttpGet.class))).thenReturn(response);

    // Act
    Optional<SparkYarnApplicationMetadata> result =
        extractor.extract("http://log-url", "test-cluster", "app-id");

    // Assert
    assertTrue(result.isPresent());
    assertEquals("test-cluster", result.get().getClusterName());
    assertEquals("app-id", result.get().getApplicationId());
    assertEquals("2.4.5", result.get().getSparkVersion());
    assertEquals("unknown", result.get().getSparkApplicationType());
  }

  @Test
  public void extract_pysparkLog_returnsPySparkMetadata() throws Exception {
    // Arrange
    String logContent =
        "{\"Event\": \"SparkListenerLogStart\", \"Spark Version\": \"3.2.0\"}\n"
            + "{\"Event\": \"SparkListenerEnvironmentUpdate\", \"Spark Properties\": {\"spark.yarn.primary.py.file\": \"script.py\"}}\n";
    byte[] zipData = createZipWithLog(logContent);

    mockResponse(zipData);

    // Act
    Optional<SparkYarnApplicationMetadata> result =
        extractor.extract("http://log-url", "test-cluster", "app-id");

    // Assert
    assertTrue(result.isPresent());
    assertEquals("test-cluster", result.get().getClusterName());
    assertEquals("PySpark", result.get().getSparkApplicationType());
  }

  @Test
  public void extract_scalaJarLog_returnsScalaJavaMetadata() throws Exception {
    // Arrange
    String logContent =
        "{\"Event\": \"SparkListenerLogStart\", \"Spark Version\": \"3.2.0\"}\n"
            + "{\"Event\": \"SparkListenerEnvironmentUpdate\", \"Spark Properties\": {\"sun.java.command\": \"com.example.MyApp --jar myapp.jar\"}}\n";
    byte[] zipData = createZipWithLog(logContent);

    mockResponse(zipData);

    // Act
    Optional<SparkYarnApplicationMetadata> result =
        extractor.extract("http://log-url", "test-cluster", "app-id");

    // Assert
    assertTrue(result.isPresent());
    assertEquals("test-cluster", result.get().getClusterName());
    assertEquals("Scala/Java", result.get().getSparkApplicationType());
  }

  @Test
  public void extract_invalidJsonLine_skipsAndContinues() throws Exception {
    // Arrange
    String logContent =
        "invalid json\n" + "{\"Event\": \"SparkListenerLogStart\", \"Spark Version\": \"3.3.0\"}\n";
    byte[] zipData = createZipWithLog(logContent);

    mockResponse(zipData);

    // Act
    Optional<SparkYarnApplicationMetadata> result =
        extractor.extract("http://log-url", "test-cluster", "app-id");

    // Assert
    assertTrue(result.isPresent());
    assertEquals("test-cluster", result.get().getClusterName());
    assertEquals("3.3.0", result.get().getSparkVersion());
  }

  @Test
  public void extract_exceedsMaxLines_returnsPartialMetadata() throws Exception {
    // Arrange
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 60; i++) {
      sb.append("{\"Event\": \"OtherEvent\"}\n");
    }
    sb.append("{\"Event\": \"SparkListenerLogStart\", \"Spark Version\": \"3.1.1\"}\n");
    byte[] zipData = createZipWithLog(sb.toString());

    mockResponse(zipData);

    // Act
    Optional<SparkYarnApplicationMetadata> result =
        extractor.extract("http://log-url", "test-cluster", "app-id");

    // Assert
    // It should not find SparkListenerLogStart because it's after 50 lines
    assertFalse(result.isPresent());
  }

  private void mockResponse(byte[] zipData) throws IOException {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(response.getStatusLine()).thenReturn(statusLine);

    HttpEntity entity = mock(HttpEntity.class);
    when(entity.getContentLength()).thenReturn((long) zipData.length);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream(zipData));
    when(response.getEntity()).thenReturn(entity);

    when(httpClient.execute(any(HttpGet.class))).thenReturn(response);
  }

  private byte[] createZipWithLog(String logContent) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(baos)) {
      ZipEntry entry = new ZipEntry("eventLog");
      zos.putNextEntry(entry);
      zos.write(logContent.getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }
    return baos.toByteArray();
  }
}
