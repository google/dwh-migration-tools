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
import com.google.common.base.Stopwatch;
import com.google.edwmigration.dumper.application.dumper.metrics.DumperRunMetrics;
import com.google.edwmigration.dumper.application.dumper.metrics.TaskDetailedSummary;
import com.google.edwmigration.dumper.application.dumper.metrics.TaskExecutionSummary;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import net.harawata.appdirs.AppDirs;
import net.harawata.appdirs.AppDirsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumperRunMetricsGenerator {

  private static final Logger logger = LoggerFactory.getLogger(DumperRunMetricsGenerator.class);
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

  public void generateRunMetrics(
      FileSystem fileSystem,
      ConnectorArguments arguments,
      TaskSetState state,
      Stopwatch stopwatch,
      boolean requiredTaskSucceeded)
      throws IOException {

    DumperRunMetrics currentDumperRunMetrics =
        generateCurrentDumperRunMetrics(arguments, state, stopwatch, requiredTaskSucceeded);

    String cacheDir = createDirPathIfNotExist();

    Path PathToCachedCumulativeSummary = Paths.get(cacheDir + ALL_DUMPER_RUN_METRICS);

    try {
      String serializedMetrics = MAPPER.writeValueAsString(currentDumperRunMetrics);
      appendToCacheOnDisk(PathToCachedCumulativeSummary, serializedMetrics);
      copyCachedAsCurrent(fileSystem, PathToCachedCumulativeSummary);
    } catch (JsonProcessingException e) {
      logger.warn("Failed to serialize dumperRunMetrics", e);
    }
  }

  private String createDirPathIfNotExist() throws IOException {
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

    return cacheDir;
  }

  private static void copyCachedAsCurrent(FileSystem zipFs, Path externalLogPath) {
    Path snapshotInZipPath = zipFs.getPath(DUMPER_RUN_METRICS);
    try {
      Path parentInZip = snapshotInZipPath.getParent();
      if (parentInZip != null && java.nio.file.Files.notExists(parentInZip)) {
        java.nio.file.Files.createDirectories(parentInZip);
      }
      java.nio.file.Files.copy(
          externalLogPath, snapshotInZipPath, StandardCopyOption.REPLACE_EXISTING);
      logger.debug(
          "Copied cumulative run summary from {} to {} in the output ZIP.",
          externalLogPath,
          snapshotInZipPath);
    } catch (IOException e) {
      logger.warn(
          "Failed to copy cumulative summary log {} to ZIP at {}",
          externalLogPath,
          snapshotInZipPath,
          e);
    }
  }

  /**
   * Generates a list of strings representing the summary for the current run. This summary does NOT
   * include the final ZIP file size, as it's generated before the ZIP is closed.
   */
  private DumperRunMetrics generateCurrentDumperRunMetrics(
      ConnectorArguments arguments, TaskSetState state, Stopwatch stopwatch, boolean success) {

    List<TaskExecutionSummary> taskExecutionSummaries =
        state.getTasksReports().stream()
            .map(
                tasksReport ->
                    new TaskExecutionSummary(tasksReport.count(), tasksReport.state().name()))
            .collect(Collectors.toList());

    List<TaskDetailedSummary> taskDetailedSummaries =
        state.getTaskResultSummaries().stream()
            .map(
                item ->
                    new TaskDetailedSummary(
                        item.getTask().getName(),
                        item.getTask().getCategory().name(),
                        item.getTaskState().name(),
                        item.getThrowable().isPresent()
                            ? item.getThrowable().get().getMessage()
                            : null))
            .collect(Collectors.toList());

    Duration elapsed = stopwatch.elapsed();

    return DumperRunMetrics.builder()
        .setId(UUID.randomUUID().toString())
        .setRunStartTime(LocalDateTime.now().minus(elapsed))
        .setRunDurationInSeconds(elapsed.getSeconds())
        .setOverallStatus(success ? "SUCCESS" : "FAILURE")
        .setTaskExecutionSummary(taskExecutionSummaries)
        .setTaskDetailedSummary(taskDetailedSummaries)
        .setArguments(arguments)
        .build();
  }

  /** Appends the given summary lines for the current run to the external log file. */
  private void appendToCacheOnDisk(Path externalLogPath, String summaryLines) {
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
