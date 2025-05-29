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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Stopwatch;
import com.google.edwmigration.dumper.application.dumper.metrics.CriticalUserJourneyMetric;
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

public class RunSummaryGenerator {

  private static final Logger logger = LoggerFactory.getLogger(RunSummaryGenerator.class);
  private static final String CACHED_CUMULATIVE_SUMMARY_FILENAME =
      "dumper_cumulative_run_summary.jsonl";
  private static final String CURRENT_SUMMARY_SNAPSHOT_FILENAME = "run_summary_snapshot.jsonl";
  private static final ObjectMapper MAPPER = createObjectMapper();

  private static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();

    mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    return mapper;
  }

  public void generateSummary(
      FileSystem fileSystem,
      ConnectorArguments arguments,
      TaskSetState state,
      Stopwatch stopwatch,
      boolean requiredTaskSucceeded) {

    try {
      String currentRunSummaryYaml =
          generateCurrentRunSummaryYaml(arguments, state, stopwatch, requiredTaskSucceeded);

      String cacheDir = createDirPathIfNotExist();

      Path PathToCachedCumulativeSummary = Paths.get(cacheDir + CACHED_CUMULATIVE_SUMMARY_FILENAME);
      appendToCachedSummary(PathToCachedCumulativeSummary, currentRunSummaryYaml);
      copyCachedAsCurrent(fileSystem, PathToCachedCumulativeSummary);
    } catch (IOException e) {
      logger.error("Failed to generate YAML summary", e);
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
    Path snapshotInZipPath = zipFs.getPath(CURRENT_SUMMARY_SNAPSHOT_FILENAME);
    try {
      Path parentInZip = snapshotInZipPath.getParent();
      if (parentInZip != null && java.nio.file.Files.notExists(parentInZip)) {
        java.nio.file.Files.createDirectories(parentInZip);
      }
      java.nio.file.Files.copy(
          externalLogPath, snapshotInZipPath, StandardCopyOption.REPLACE_EXISTING);
      logger.info(
          "Copied cumulative run summary from {} to {} in the output ZIP.",
          externalLogPath,
          snapshotInZipPath);
    } catch (IOException e) {
      logger.error(
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
  private String generateCurrentRunSummaryYaml(
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

    CriticalUserJourneyMetric CujMetrics =
        CriticalUserJourneyMetric.builder()
            .setId(UUID.randomUUID().toString())
            .setRunStartTime(LocalDateTime.now().minus(elapsed))
            .setRunDurationInSeconds(elapsed.getSeconds())
            .setOverallStatus(success ? "SUCCESS" : "FAILURE")
            .setTaskExecutionSummary(taskExecutionSummaries)
            .setTaskDetailedSummary(taskDetailedSummaries)
            .setArguments(arguments)
            .build();

    try {
      return MAPPER.writeValueAsString(CujMetrics);
    } catch (IOException e) {
      logger.error("Failed to generate YAML summary", e);
      return "error: \"Failed to generate YAML summary: "
          + e.getMessage().replace("\"", "\\\"")
          + "\""; // Basic YAML error
    }
  }

  /** Appends the given summary lines for the current run to the external log file. */
  private void appendToCachedSummary(Path externalLogPath, String summaryLines) {
    try (BufferedWriter writer =
            java.nio.file.Files.newBufferedWriter(
                externalLogPath,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
        PrintWriter printer = new PrintWriter(writer)) {

      printer.println(summaryLines);
      printer.flush();
    } catch (IOException e) {
      logger.error("Failed to append to external cumulative summary log: {}", externalLogPath, e);
      System.err.println(
          "ERROR: Could not write to external cumulative summary log "
              + externalLogPath
              + ": "
              + e.getMessage());
    }
  }
}
