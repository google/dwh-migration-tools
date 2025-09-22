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
import com.google.edwmigration.dumper.application.dumper.metrics.*;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import net.harawata.appdirs.AppDirs;
import net.harawata.appdirs.AppDirsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Strategy implementation that writes telemetry data. This replaces the behavior when shouldWrite =
 * true.
 */
public class DiskTelemetryWriteStrategy implements TelemetryWriteStrategy {
  private static final Logger logger = LoggerFactory.getLogger(DiskTelemetryWriteStrategy.class);
  private static final String ALL_DUMPER_RUN_METRICS = "all-dumper-telemetry.jsonl";
  private static final String DUMPER_RUN_METRICS = "dumper-telemetry.jsonl";
  private static final Path TELEMETRY_OS_CACHE_PATH =
      Paths.get(createTelemetryOsDirIfNotExists(), ALL_DUMPER_RUN_METRICS);
  private static final ObjectMapper MAPPER = createObjectMapper();
  private final List<ClientTelemetry> bufferOs = new ArrayList<>();
  private final List<ClientTelemetry> bufferZip = new ArrayList<>();
  private FileSystem fileSystem;
  private static boolean telemetryOsCacheIsAvailable = true;

  public DiskTelemetryWriteStrategy() {}

  private static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();

    mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    return mapper;
  }

  public void setZipFilePath(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
    if (copyOsCacheToZip() && telemetryOsCacheIsAvailable) {
      // these events were already registered in OsCache
      bufferZip.clear();
    }
    flush();
  }

  @Override
  public void process(ClientTelemetry clientTelemetry) {
    logger.debug(
        "Processing telemetry data with {} payload items", clientTelemetry.getPayload().size());

    if (telemetryOsCacheIsAvailable) {
      bufferOs.add(clientTelemetry);
    }
    bufferZip.add(clientTelemetry);

    flush();
  }

  @Override
  public void flush() {
    // this implementation uses buffer until zip file is created afterwords it is flushed per
    // process
    flushOsCache();
    flushZip();
  }

  private void flushOsCache() {
    if (!telemetryOsCacheIsAvailable) {
      return;
    }

    bufferOs.forEach(
        ct -> {
          try {
            writeOnDisk(TELEMETRY_OS_CACHE_PATH, MAPPER.writeValueAsString(ct));
          } catch (JsonProcessingException e) {
            logger.warn("Failed to serialize telemetry to write in Os Cache", e);
          }
        });
    bufferOs.clear();
  }

  private void flushZip() {
    if (fileSystem == null) {
      return;
    }

    bufferZip.forEach(
        ct -> {
          try {
            writeOnDisk(fileSystem.getPath(DUMPER_RUN_METRICS), MAPPER.writeValueAsString(ct));
          } catch (JsonProcessingException e) {
            logger.warn("Failed to serialize telemetry to write in Zip", e);
          }
        });
    bufferZip.clear();
  }

  private static String createTelemetryOsDirIfNotExists() {
    AppDirs appDirs = AppDirsFactory.getInstance();

    String appName = "DWH-Dumper";
    String appVersion = ""; // All versions are accumulated in the same file
    String appAuthor = "google"; // Optional, can be null
    String cacheDir = appDirs.getUserCacheDir(appName, appVersion, appAuthor);
    Path applicationCacheDirPath = Paths.get(cacheDir);
    if (java.nio.file.Files.notExists(applicationCacheDirPath)) {
      try {
        java.nio.file.Files.createDirectories(applicationCacheDirPath);
        logger.info("Created application telemetry cache directory: {}", applicationCacheDirPath);
      } catch (IOException e) {
        disableOsCache();
        logger.warn(
            "Unable to create application telemetry cache directory : {}", applicationCacheDirPath);
      }
    }

    return cacheDir;
  }

  private boolean copyOsCacheToZip() {
    Path snapshotInZipPath = fileSystem.getPath(DUMPER_RUN_METRICS);
    try {
      Path parentInZip = snapshotInZipPath.getParent();
      if (parentInZip != null && java.nio.file.Files.notExists(parentInZip)) {
        java.nio.file.Files.createDirectories(parentInZip);
      }
      java.nio.file.Files.copy(
          TELEMETRY_OS_CACHE_PATH, snapshotInZipPath, StandardCopyOption.REPLACE_EXISTING);
      logger.debug(
          "Copied Cached {} telemetry to zip file {}.", TELEMETRY_OS_CACHE_PATH, snapshotInZipPath);
      return true;
    } catch (IOException e) {
      logger.warn(
          "Failed to copy cached telemetry from {} to ZIP at {}",
          TELEMETRY_OS_CACHE_PATH,
          snapshotInZipPath,
          e);
      return false;
    }
  }

  /** Appends the given summary lines for the current run to the external log file. */
  private static void writeOnDisk(Path path, String summaryLines) {
    try (BufferedWriter writer =
            newBufferedWriter(
                path,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
        PrintWriter printer = new PrintWriter(writer)) {

      printer.println(summaryLines);
      printer.flush();
    } catch (IOException e) {
      logger.warn("Failed to append to external cumulative summary log: {}", path, e);
    }
  }

  private static void disableOsCache() {
    telemetryOsCacheIsAvailable = false;
  }
}
