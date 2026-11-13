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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.exception.SparkHistoryConnectionException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.exception.SparkLogFormatException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkApplicationType;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkYarnApplicationMetadata;
import java.io.BufferedReader;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import javax.annotation.Nullable;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkJobMetadataExtractor {

  private static final Logger logger = LoggerFactory.getLogger(SparkJobMetadataExtractor.class);
  // The server may ignore the Range header and send the whole log; the limits below still apply.
  private static final long REQUESTED_BYTES = 1024L * 10;
  // Upper bound on the bytes read from the response body.
  static final long MAX_RESPONSE_BYTES = 8L * 1024 * 1024;
  // Upper bound on the decompressed bytes read from the event log.
  static final long MAX_LOG_BYTES = 8L * 1024 * 1024;
  // Longer lines are skipped. An environment update event is usually well below 1 MiB.
  static final int MAX_LINE_LENGTH = 2 * 1024 * 1024;
  static final int MAX_LINES_TO_PARSE = 50;

  private final ObjectMapper mapper;
  private final CloseableHttpClient httpClient;

  public SparkJobMetadataExtractor(ObjectMapper mapper, CloseableHttpClient httpClient) {
    this.mapper = mapper;
    this.httpClient = httpClient;
  }

  public Optional<SparkYarnApplicationMetadata> extract(
      URI logUrl, String clusterName, String applicationId)
      throws SparkHistoryConnectionException, SparkLogFormatException {

    HttpGet request = new HttpGet(logUrl);
    request.setHeader("Range", String.format("bytes=0-%d", REQUESTED_BYTES));

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      int status = response.getStatusLine().getStatusCode();

      if (status == HttpStatus.SC_NOT_FOUND) {
        return Optional.empty();
      }

      if (status != HttpStatus.SC_OK && status != HttpStatus.SC_PARTIAL_CONTENT) {
        throw new SparkHistoryConnectionException(
            String.format("Failed to fetch log for app %s. Status: %d", applicationId, status));
      }

      if (response.getEntity().getContentLength() == 0) {
        return Optional.empty();
      }

      try (InputStream stream = getResponseContent(response);
          BufferedReader reader =
              new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
        return parseApplicationLog(reader, clusterName, applicationId);
      } catch (ResponseTooLongException e) {
        throw new SparkLogFormatException(
            String.format(
                "Log response for app %s is longer than %d bytes.",
                applicationId, MAX_RESPONSE_BYTES),
            e);
      } catch (ZipException e) {
        throw new SparkLogFormatException("Log stream is not a valid ZIP file.", e);
      } catch (IOException e) {
        throw new SparkHistoryConnectionException("Stream interrupted while reading log.", e);
      }
    } catch (IOException e) {
      throw new SparkHistoryConnectionException(
          "Failed to connect to History Server at " + logUrl, e);
    }
  }

  private InputStream getResponseContent(CloseableHttpResponse response)
      throws SparkLogFormatException, IOException {
    InputStream content = new BoundedResponseStream(response.getEntity().getContent());
    try {
      ZipInputStream zipStream = new ZipInputStream(content);
      ZipEntry entry = zipStream.getNextEntry();
      if (entry == null) {
        throw new SparkLogFormatException("Stream contains no ZIP entries.");
      }
      return ByteStreams.limit(zipStream, MAX_LOG_BYTES);
    } catch (IllegalArgumentException e) {
      throw new SparkLogFormatException("Invalid ZIP header or format.", e);
    }
  }

  private Optional<SparkYarnApplicationMetadata> parseApplicationLog(
      BufferedReader reader, String clusterName, String applicationId) throws IOException {
    String version = null;
    SparkApplicationType applicationType = null;

    String line;
    int linesRead = 0;

    while (linesRead < MAX_LINES_TO_PARSE && (line = readLine(reader)) != null) {
      linesRead++;
      if (line.length() > MAX_LINE_LENGTH) {
        logger.warn(
            "Skipped line {} of the event log of YARN application with ID: {}, it is longer than {}"
                + " characters.",
            linesRead,
            applicationId,
            MAX_LINE_LENGTH);
        continue;
      }

      if (line.trim().isEmpty()) {
        continue;
      }

      try {
        JsonNode rootNode = mapper.readTree(line);
        if (!rootNode.has("Event")) {
          continue;
        }

        String eventType = rootNode.get("Event").asText();
        if ("SparkListenerLogStart".equals(eventType) && rootNode.has("Spark Version")) {
          version = rootNode.get("Spark Version").asText();
        }
        if ("SparkListenerEnvironmentUpdate".equals(eventType)) {
          applicationType = SparkApplicationTypeDetector.detect(rootNode);
        }

        if (version != null && applicationType != null) {
          break;
        }

      } catch (Exception e) {
        logger.warn(
            String.format(
                "Error occurred during parsing event log of YARN application with ID: %s",
                applicationId),
            e);
      }
    }

    if (version == null && applicationType == null) {
      return Optional.empty();
    }

    return Optional.of(
        SparkYarnApplicationMetadata.create(
            clusterName,
            applicationId,
            version != null ? version : "unknown",
            (applicationType != null ? applicationType : SparkApplicationType.UNKNOWN)
                .getDisplayName()));
  }

  /**
   * Reads a line like {@link BufferedReader#readLine()}, but keeps at most {@code MAX_LINE_LENGTH +
   * 1} characters of it, so a line over the limit is returned truncated and still over the limit.
   */
  @Nullable
  private static String readLine(BufferedReader reader) throws IOException {
    int c = reader.read();
    if (c == -1) {
      return null;
    }

    StringBuilder line = new StringBuilder();
    while (c != -1 && c != '\n' && c != '\r') {
      if (line.length() <= MAX_LINE_LENGTH) {
        line.append((char) c);
      }

      c = reader.read();
    }

    if (c == '\r') {
      reader.mark(1);
      if (reader.read() != '\n') {
        reader.reset();
      }
    }

    return line.toString();
  }

  /**
   * Reads at most {@code MAX_RESPONSE_BYTES} of the response body and leaves the body open when
   * closed. Closing the body would make the HTTP client read the rest of it; closing the response
   * discards the connection instead.
   */
  private static class BoundedResponseStream extends FilterInputStream {

    private long remaining = MAX_RESPONSE_BYTES;

    BoundedResponseStream(InputStream content) {
      super(content);
    }

    @Override
    public int read() throws IOException {
      checkRemaining();
      int result = in.read();
      if (result != -1) {
        remaining--;
      }

      return result;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      checkRemaining();
      int result = in.read(buffer, offset, (int) Math.min(length, remaining));
      if (result != -1) {
        remaining -= result;
      }

      return result;
    }

    @Override
    public long skip(long count) throws IOException {
      checkRemaining();
      long skipped = in.skip(Math.min(count, remaining));
      remaining -= skipped;
      return skipped;
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    @Override
    public void close() {}

    private void checkRemaining() throws ResponseTooLongException {
      if (remaining == 0) {
        throw new ResponseTooLongException();
      }
    }
  }

  private static class ResponseTooLongException extends IOException {}
}
