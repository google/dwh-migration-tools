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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.base.Stopwatch;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import net.harawata.appdirs.AppDirs;
import net.harawata.appdirs.AppDirsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SummaryYamlGenerator {

  private static final Logger logger = LoggerFactory.getLogger(SummaryYamlGenerator.class);
  private static final String CACHED_CUMULATIVE_SUMMARY_FILENAME =
      "dumper_cumulative_run_summary.yaml";
  private static final String CURRENT_SUMMARY_SNAPSHOT_FILENAME = "run_summary_snapshot.yaml";
  private static final ObjectMapper YAML_MAPPER =
      new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

  private static final DateTimeFormatter ISO_TIMESTAMP_FORMATTER =
      DateTimeFormatter.ISO_LOCAL_DATE_TIME;

  // The method could be static if it doesn't need instance state,
  // or the class could be instantiated by MetadataDumper.
  public void generateSummaryYaml(
      FileSystem fileSystem,
      ConnectorArguments arguments,
      TaskSetState state,
      Stopwatch stopwatch,
      String outputFileLocation,
      boolean requiredTaskSucceeded) {

    try {
      String currentRunSummaryYaml =
          generateCurrentRunSummaryYaml(
              arguments, state, stopwatch, outputFileLocation, requiredTaskSucceeded);

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
      // Ensure parent directory exists in ZIP (though for a root file, it's not needed)
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
      ConnectorArguments arguments,
      TaskSetState state,
      Stopwatch stopwatch,
      String outputFileLocation, // The target ZIP file path
      boolean success) {
    ArrayNode rootArrayNode = YAML_MAPPER.createArrayNode();
    ObjectNode rootNode = YAML_MAPPER.createObjectNode();

    rootNode.put("id", UUID.randomUUID().toString());
    rootNode.put("logTime", LocalDateTime.now().toString());
    rootNode.put("runDuration", stopwatch.toString());
    ArrayNode taskReportsNode = rootNode.putArray("taskExecutionSummary");
    state
        .getTasksReports()
        .forEach(
            taskReport -> {
              ObjectNode reportNode = taskReportsNode.addObject();
              reportNode.put("count", taskReport.count());
              reportNode.put("state", taskReport.state().name()); // Or .toString()
            });

    ArrayNode taskResultSummariesNode = rootNode.putArray("taskResultSummaries");
    for (TaskSetState.TaskResultSummary item : state.getTaskResultSummaries()) {
      ObjectNode itemNode = YAML_MAPPER.createObjectNode();

      itemNode.put("type", item.getTask().getCategory().name());
      itemNode.put("status", item.getTaskState().name());
      itemNode.put(
          "detail",
          item.getThrowable().isPresent()
              ? item.getThrowable().get().getMessage()
              : item.getTask().toString());

      taskResultSummariesNode.add(itemNode);
    }

    ObjectNode argsNode = YAML_MAPPER.createObjectNode();
    argsNode.put("connector", arguments.getConnectorName());
    argsNode.put("host", arguments.getHost());
    argsNode.put("port", arguments.getPort());
    argsNode.put("warehouse", arguments.getWarehouse());
    argsNode.put("user", arguments.getUser());
    argsNode.put("output", arguments.getOutputFile().orElse(null));
    argsNode.put("query-log-days", arguments.getQueryLogDays());
    argsNode.put(
        "query-log-start",
        arguments.getQueryLogStart() == null ? null : arguments.getQueryLogStart().toString());
    argsNode.put(
        "query-log-end",
        arguments.getQueryLogEnd() == null ? null : arguments.getQueryLogEnd().toString());
    argsNode.put("assessment", arguments.isAssessment());

    argsNode.putPOJO("driver", arguments.getDriverClass());
    argsNode.putPOJO("database", arguments.getDatabases());
    argsNode.putPOJO("config", arguments.getConfiguration());
    argsNode.putPOJO("query-log-alternates", arguments.getQueryLogAlternates());

    rootNode.set("arguments", argsNode);

    // --- End of arguments structuring ---
    rootNode.put("targetOutputZip", outputFileLocation);
    rootNode.put("overallStatus", success ? "SUCCESS" : "FAILURE");

    rootArrayNode.add(rootNode);
    try {
      return YAML_MAPPER.writeValueAsString(rootArrayNode);
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

      printer.print(summaryLines);
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
