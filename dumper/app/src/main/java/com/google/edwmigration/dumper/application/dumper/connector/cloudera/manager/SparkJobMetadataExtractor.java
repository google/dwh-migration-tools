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
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.exception.SparkHistoryConnectionException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.exception.SparkLogFormatException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkApplicationType;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkYarnApplicationMetadata;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkJobMetadataExtractor {

  private static final Logger logger = LoggerFactory.getLogger(SparkJobMetadataExtractor.class);
  private static final long MAX_BYTES_TO_PARSE = 1024L * 10;
  private static final int MAX_LINES_TO_PARSE = 50;

  private final ObjectMapper mapper;
  private final CloseableHttpClient httpClient;

  public SparkJobMetadataExtractor(ObjectMapper mapper, CloseableHttpClient httpClient) {
    this.mapper = mapper;
    this.httpClient = httpClient;
  }

  public Optional<SparkYarnApplicationMetadata> extract(
      String logUrl, String clusterName, String applicationId)
      throws SparkHistoryConnectionException, SparkLogFormatException {

    HttpGet request = new HttpGet(logUrl);
    request.setHeader("Range", String.format("bytes=0-%d", MAX_BYTES_TO_PARSE));

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
    try {
      ZipInputStream zipStream = new ZipInputStream(response.getEntity().getContent());
      ZipEntry entry = zipStream.getNextEntry();
      if (entry == null) {
        throw new SparkLogFormatException("Stream contains no ZIP entries.");
      }
      return zipStream;
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

    while ((line = reader.readLine()) != null && linesRead < MAX_LINES_TO_PARSE) {
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
      linesRead++;
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
}
