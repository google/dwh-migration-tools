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
package com.google.edwmigration.dumper.application.dumper;

import static java.nio.file.Files.newBufferedWriter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.edwmigration.dumper.application.dumper.metrics.ClientTelemetry;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import net.harawata.appdirs.AppDirs;
import net.harawata.appdirs.AppDirsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelemetryWriter {

  private static final Logger logger = LoggerFactory.getLogger(TelemetryWriter.class);
  private static final String ALL_DUMPER_RUN_METRICS = "all-dumper-telemetry.jsonl";
  private static final String DUMPER_RUN_METRICS = "dumper-telemetry.jsonl";
  private static final ObjectMapper MAPPER = createObjectMapper();

  private static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();

    mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    return mapper;
  }

  public static void write(FileSystem fileSystem, ClientTelemetry clientTelemetry)
      throws IOException {

    write(fileSystem, clientTelemetry, createDirPathIfNotExist());
  }

  static void write(FileSystem fileSystem, ClientTelemetry clientTelemetry, Path cacheDir) {
    Path PathToCachedCumulativeSummary = cacheDir.resolve(ALL_DUMPER_RUN_METRICS);

    try {
      String serializedMetrics = MAPPER.writeValueAsString(clientTelemetry);
      appendToCacheOnDisk(PathToCachedCumulativeSummary, serializedMetrics);
      writeCurrentRunToZip(fileSystem, serializedMetrics);
    } catch (JsonProcessingException e) {
      logger.warn("Failed to serialize dumperRunMetrics", e);
    }
  }

  private static Path createDirPathIfNotExist() throws IOException {
    AppDirs appDirs = AppDirsFactory.getInstance();

    String appName = "DWH-Dumper";
    String appVersion = ""; // All versions are accumulated in the same file
    String appAuthor = "google"; // Optional, can be null
    String cacheDir = appDirs.getUserCacheDir(appName, appVersion, appAuthor);
    Path applicationCacheDirPath = Paths.get(cacheDir);
    if (java.nio.file.Files.notExists(applicationCacheDirPath)) {
      java.nio.file.Files.createDirectories(applicationCacheDirPath);
      logger.info("Created application cache directory: {}", applicationCacheDirPath);
    }

    return applicationCacheDirPath;
  }

  /** Writes the record of the current run, and no earlier runs, into the output ZIP. */
  private static void writeCurrentRunToZip(FileSystem zipFs, String serializedMetrics) {
    Path snapshotInZipPath = zipFs.getPath(DUMPER_RUN_METRICS);
    try (BufferedWriter writer = newBufferedWriter(snapshotInZipPath, StandardCharsets.UTF_8)) {
      writer.write(serializedMetrics);
      writer.newLine();
      logger.debug("Wrote current run summary to {} in the output ZIP.", snapshotInZipPath);
    } catch (IOException e) {
      logger.warn("Failed to write current run summary to ZIP at {}", snapshotInZipPath, e);
    }
  }

  /** Appends the given summary lines for the current run to the external log file. */
  private static void appendToCacheOnDisk(Path externalLogPath, String summaryLines) {
    try (BufferedWriter writer =
            newBufferedWriter(
                externalLogPath,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
        PrintWriter printer = new PrintWriter(writer)) {

      printer.println(summaryLines);
      printer.flush();
    } catch (IOException e) {
      logger.warn("Failed to append to external cumulative summary log: {}", externalLogPath, e);
    }
  }
}
