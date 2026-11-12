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
import static com.google.common.base.Strings.repeat;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.SparkJobMetadataExtractor.MAX_LINES_TO_PARSE;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.SparkJobMetadataExtractor.MAX_LINE_LENGTH;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.SparkJobMetadataExtractor.MAX_LOG_BYTES;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.SparkJobMetadataExtractor.MAX_RESPONSE_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.exception.SparkHistoryConnectionException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.exception.SparkLogFormatException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkYarnApplicationMetadata;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SparkJobMetadataExtractorTest {

  private static final String LOG_PATH = "/applications/app-id/logs";
  private static final String LOG_START_LINE =
      "{\"Event\": \"SparkListenerLogStart\", \"Spark Version\": \"3.1.1\"}\n";
  private static final String ENVIRONMENT_UPDATE_LINE =
      "{\"Event\": \"SparkListenerEnvironmentUpdate\", \"Spark Properties\": {\"spark.app.name\": \"SparkSQL\"}}\n";

  private static WireMockServer server;

  private final ObjectMapper mapper = new ObjectMapper();
  private CloseableHttpClient httpClient;
  private SparkJobMetadataExtractor extractor;
  private CloseableHttpClient serverClient;
  private SparkJobMetadataExtractor serverExtractor;

  @BeforeClass
  public static void startServer() {
    server =
        new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort().gzipDisabled(true));
    server.start();
  }

  @AfterClass
  public static void stopServer() {
    server.stop();
  }

  @Before
  public void setUp() {
    httpClient = mock(CloseableHttpClient.class);
    extractor = new SparkJobMetadataExtractor(mapper, httpClient);
    server.resetAll();
    serverClient = HttpClients.createDefault();
    serverExtractor = new SparkJobMetadataExtractor(mapper, serverClient);
  }

  @After
  public void tearDown() throws IOException {
    serverClient.close();
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
        extractor.extract(toUri("http://log-url"), "test-cluster", "app-id");

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
        () -> extractor.extract(toUri("http://log-url"), "test-cluster", "app-id"));
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
        extractor.extract(toUri("http://log-url"), "test-cluster", "app-id");

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
        () -> extractor.extract(toUri("http://log-url"), "test-cluster", "app-id"));
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
        extractor.extract(toUri("http://log-url"), "test-cluster", "app-id");

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
        extractor.extract(toUri("http://log-url"), "test-cluster", "app-id");

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
        extractor.extract(toUri("http://log-url"), "test-cluster", "app-id");

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
            + "{\"Event\": \"SparkListenerEnvironmentUpdate\", \"System Properties\": {\"sun.java.command\": \"com.example.MyApp --jar myapp.jar\"}}\n";
    byte[] zipData = createZipWithLog(logContent);

    mockResponse(zipData);

    // Act
    Optional<SparkYarnApplicationMetadata> result =
        extractor.extract(toUri("http://log-url"), "test-cluster", "app-id");

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
        extractor.extract(toUri("http://log-url"), "test-cluster", "app-id");

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
        extractor.extract(toUri("http://log-url"), "test-cluster", "app-id");

    // Assert
    // It should not find SparkListenerLogStart because it's after 50 lines
    assertFalse(result.isPresent());
  }

  @Test
  public void extract_serverSendsNormalLog_returnsMetadata() throws Exception {
    StringBuilder hadoopProperties = new StringBuilder();
    for (int i = 0; i < 3000; i++) {
      hadoopProperties.append(String.format("\"hadoop.property.%d\": \"value-%d\", ", i, i));
    }
    StringBuilder classpath = new StringBuilder();
    for (int i = 0; i < 500; i++) {
      classpath.append(
          String.format("\"/opt/cloudera/parcels/CDH/jars/lib-%d.jar\": \"System\", ", i));
    }
    StringBuilder log = new StringBuilder();
    log.append("{\"Event\": \"SparkListenerLogStart\", \"Spark Version\": \"3.3.2\"}\n");
    log.append("{\"Event\": \"SparkListenerResourceProfileAdded\", \"Resource Profile Id\": 0}\n");
    log.append("{\"Event\": \"SparkListenerEnvironmentUpdate\"")
        .append(", \"Spark Properties\": {\"spark.app.name\": \"SparkSQL\"}")
        .append(", \"Hadoop Properties\": {")
        .append(hadoopProperties)
        .append("\"last\": \"\"}")
        .append(", \"Classpath Entries\": {")
        .append(classpath)
        .append("\"last\": \"System\"}}\n");
    for (int i = 0; i < 10_000; i++) {
      log.append(String.format("{\"Event\": \"SparkListenerTaskEnd\", \"Task ID\": %d}\n", i));
    }
    stubLog(createZipWithLog(log.toString()));

    Optional<SparkYarnApplicationMetadata> result = extractFromServer();

    assertTrue(result.isPresent());
    assertEquals("3.3.2", result.get().getSparkVersion());
    assertEquals("SparkSQL", result.get().getSparkApplicationType());
  }

  @Test(timeout = 15_000)
  public void extract_serverSendsWholeLogSlowly_stopsReadingAtLineLimit() throws Exception {
    StringBuilder log = new StringBuilder();
    for (int i = 0; i < 100_000; i++) {
      log.append(String.format("{\"Event\": \"SparkListenerTaskEnd\", \"Task ID\": %d}\n", i));
    }
    log.append(LOG_START_LINE);
    byte[] body = createZipWithLog(log.toString());
    server.stubFor(
        get(LOG_PATH)
            .willReturn(
                aResponse().withStatus(200).withBody(body).withChunkedDribbleDelay(50, 40_000)));

    Optional<SparkYarnApplicationMetadata> result = extractFromServer();

    assertFalse(result.isPresent());
  }

  @Test
  public void extract_responseLongerThanLimit_throwsException() throws Exception {
    String paddingLine = paddingLine(MAX_LINE_LENGTH / 2);
    StringBuilder log = new StringBuilder();
    while (log.length() <= MAX_RESPONSE_BYTES) {
      log.append(paddingLine);
    }
    log.append(LOG_START_LINE);
    stubLog(createStoredZipWithLog(log.toString()));

    assertThrows(SparkLogFormatException.class, this::extractFromServer);
  }

  @Test
  public void extract_logLongerThanLimit_stopsReading() throws Exception {
    String paddingLine = paddingLine(MAX_LINE_LENGTH);
    StringBuilder log = new StringBuilder();
    while (log.length() <= MAX_LOG_BYTES) {
      log.append(paddingLine);
    }
    log.append(LOG_START_LINE);
    stubLog(createZipWithLog(log.toString()));

    Optional<SparkYarnApplicationMetadata> result = extractFromServer();

    assertFalse(result.isPresent());
  }

  @Test
  public void extract_lineLongerThanLimit_skipsLine() throws Exception {
    String logStartPrefix =
        "{\"Event\": \"SparkListenerLogStart\", \"Spark Version\": \"3.1.1\", \"x\": \"";
    String suffix = "\"}";
    String longLine =
        logStartPrefix
            + repeat("a", MAX_LINE_LENGTH + 1 - logStartPrefix.length() - suffix.length())
            + suffix
            + "\n";
    stubLog(createZipWithLog(longLine + ENVIRONMENT_UPDATE_LINE));

    Optional<SparkYarnApplicationMetadata> result = extractFromServer();

    assertTrue(result.isPresent());
    assertEquals("unknown", result.get().getSparkVersion());
    assertEquals("SparkSQL", result.get().getSparkApplicationType());
  }

  @Test
  public void extract_blankLinesUpToLineLimit_stopsReading() throws Exception {
    String log = repeat("\n", MAX_LINES_TO_PARSE) + LOG_START_LINE;
    stubLog(createZipWithLog(log));

    Optional<SparkYarnApplicationMetadata> result = extractFromServer();

    assertFalse(result.isPresent());
  }

  @Test
  public void extract_linesWithoutEventUpToLineLimit_stopsReading() throws Exception {
    String log = repeat("{\"Other\": 1}\n", MAX_LINES_TO_PARSE) + LOG_START_LINE;
    stubLog(createZipWithLog(log));

    Optional<SparkYarnApplicationMetadata> result = extractFromServer();

    assertFalse(result.isPresent());
  }

  private Optional<SparkYarnApplicationMetadata> extractFromServer() throws Exception {
    return serverExtractor.extract(
        URI.create(server.baseUrl() + LOG_PATH), "test-cluster", "app-id");
  }

  private static void stubLog(byte[] body) {
    server.stubFor(get(LOG_PATH).willReturn(aResponse().withStatus(200).withBody(body)));
  }

  private static String paddingLine(int length) {
    String prefix = "{\"Event\": \"Padding\", \"x\": \"";
    String suffix = "\"}";
    return prefix + repeat("a", length - prefix.length() - suffix.length()) + suffix + "\n";
  }

  private static byte[] createStoredZipWithLog(String logContent) throws IOException {
    byte[] data = logContent.getBytes(StandardCharsets.UTF_8);
    CRC32 crc = new CRC32();
    crc.update(data);
    ZipEntry entry = new ZipEntry("eventLog");
    entry.setMethod(ZipEntry.STORED);
    entry.setSize(data.length);
    entry.setCompressedSize(data.length);
    entry.setCrc(crc.getValue());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(baos)) {
      zos.putNextEntry(entry);
      zos.write(data);
      zos.closeEntry();
    }
    return baos.toByteArray();
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

  private static URI toUri(String s) {
    return URI.create(s);
  }
}
