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

import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.repeat;

import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.FileSystemOutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.ArgumentsTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcRunSQLScript;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskGroup;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState.TaskResultSummary;
import com.google.edwmigration.dumper.application.dumper.task.VersionTask;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author miguel */
public class MetadataDumper {

  private static final Logger logger = LoggerFactory.getLogger(MetadataDumper.class);

  private static final Pattern GCS_PATH_PATTERN =
      Pattern.compile("gs://(?<bucket>[^/]+)/(?<path>.*)");

  private final TelemetryProcessor telemetryProcessor;
  private final ConnectorArguments connectorArguments;
  private final ShutdownHook shutdownHook;

  public MetadataDumper(String... args) throws Exception {
    this((zipPath) -> {}, args);
  }

  public MetadataDumper(ShutdownHook shutdownHook, String... args) throws Exception {
    this.connectorArguments = new ConnectorArguments(JsonResponseFile.addResponseFiles(args));
    telemetryProcessor =
        new TelemetryProcessor(
            TelemetryStrategyFactory.createStrategy(connectorArguments.isTelemetryOn()));
    if (connectorArguments.saveResponseFile()) {
      JsonResponseFile.save(connectorArguments);
    }

    this.shutdownHook = shutdownHook;
  }

  public boolean run() throws Exception {
    String connectorName = connectorArguments.getConnectorName();

    Connector connector = ConnectorRepository.getInstance().getByName(connectorName);
    if (connector == null) {
      logger.error(
          "Target connector '{}' not supported; available are {}.",
          connectorName,
          ConnectorRepository.getInstance().getAllNames());
      return false;
    }
    connector.validate(connectorArguments);
    return run(connector);
  }

  protected boolean run(@Nonnull Connector connector) throws Exception {
    List<Task<?>> tasks = new ArrayList<>();
    tasks.add(new VersionTask());
    tasks.add(new ArgumentsTask(connectorArguments));
    {
      File sqlScript = connectorArguments.getSqlScript();
      if (sqlScript != null) {
        tasks.add(new JdbcRunSQLScript(sqlScript));
      }
    }
    connector.addTasksTo(tasks, connectorArguments);

    // The default output file is based on the connector.
    // We had a customer request to base it on the database, but that isn't well-defined,
    // as there may be 0 or N databases in a single file.
    String outputFileLocation = getOutputFileLocation(connector, connectorArguments);

    if (connectorArguments.isDryRun()) {
      String title = "Dry run: Printing task list for " + connector.getName();
      System.out.println(title);
      System.out.println(repeat('=', title.length()));
      System.out.println("Writing to " + outputFileLocation);
      for (Task<?> task : tasks) {
        print(task, 1);
      }
      return true;
    }

    Stopwatch stopwatch = Stopwatch.createStarted();
    long outputFileLength = 0;
    TaskSetState.Impl state = new TaskSetState.Impl();

    logger.info("Using connector: [{}]", connector);
    SummaryPrinter summaryPrinter = new SummaryPrinter();
    boolean requiredTaskSucceeded = false;

    try (Closer closer = Closer.create()) {
      Path outputPath = prepareOutputPath(outputFileLocation, closer, connectorArguments);

      URI outputUri = URI.create("jar:" + outputPath.toUri());

      Map<String, Object> fileSystemProperties =
          ImmutableMap.<String, Object>builder()
              .put("create", "true")
              .put("useTempFile", Boolean.TRUE)
              .build();
      FileSystem fileSystem =
          closer.register(FileSystems.newFileSystem(outputUri, fileSystemProperties));
      OutputHandleFactory sinkFactory =
          new FileSystemOutputHandleFactory(fileSystem, "/"); // It's required to be "/"
      logger.debug("Target filesystem is [{}]", sinkFactory);

      Handle handle = closer.register(connector.open(connectorArguments));

      new TasksRunner(
              sinkFactory,
              handle,
              connectorArguments.getThreadPoolSize(),
              state,
              tasks,
              connectorArguments)
          .run();

      requiredTaskSucceeded = checkRequiredTaskSuccess(summaryPrinter, state, outputFileLocation);

      telemetryProcessor.addDumperRunMetricsToPayload(
          connectorArguments, state, stopwatch, requiredTaskSucceeded);
      telemetryProcessor.processTelemetry(fileSystem);
    } finally {
      shutdownHook.shutdown(outputFileLocation);

      // We must do this in finally after the ZipFileSystem has been closed.
      File outputFile = new File(outputFileLocation);
      if (outputFile.isFile()) {
        outputFileLength = outputFile.length();
      }

      printTaskResults(summaryPrinter, state);
      logFinalSummary(
          summaryPrinter,
          state,
          outputFileLength,
          stopwatch,
          outputFileLocation,
          requiredTaskSucceeded);
    }

    return requiredTaskSucceeded;
  }

  private void print(@Nonnull Task<?> task, int indent) {
    System.out.println(repeat(' ', indent * 2) + task);
    if (task instanceof TaskGroup) {
      for (Task<?> subtask : ((TaskGroup) task).getTasks()) {
        print(subtask, indent + 1);
      }
    }
  }

  private Path prepareOutputPath(
      @Nonnull String fileName, @Nonnull Closer closer, @Nonnull ConnectorArguments arguments)
      throws IOException {
    Matcher matcher = GCS_PATH_PATTERN.matcher(fileName);
    if (matcher.matches()) {
      String bucket = matcher.group("bucket");
      String path = matcher.group("path");
      logger.debug(
          "Setting up CloudStorageFileSystem with bucket '{}' and path '{}'.", bucket, path);
      CloudStorageFileSystem cloudStorageFileSystem =
          closer.register(CloudStorageFileSystem.forBucket(bucket));
      return cloudStorageFileSystem.getPath(path);
    } else {
      Path path = Paths.get(fileName);
      File file = path.toFile();
      if (file.exists()) {
        if (!arguments.isOutputContinue()) {
          file.delete(); // It's a simple file, and we were asked to overwrite it.
        }
      } else {
        Files.createParentDirs(file);
      }
      return path;
    }
  }

  private String getOutputFileLocation(Connector connector, ConnectorArguments arguments) {
    Clock clock = Clock.systemDefaultZone();
    // The default output file is based on the connector.
    // We had a customer request to base it on the database, but that isn't well-defined,
    // as there may be 0 or N databases in a single file.
    String defaultFileName = connector.getDefaultFileName(arguments.isAssessment(), clock);
    return arguments
        .getOutputFile()
        .map(file -> getVerifiedFile(defaultFileName, file))
        .orElse(defaultFileName);
  }

  private String getVerifiedFile(String defaultFileName, String fileName) {
    Matcher gcsPathMatcher = GCS_PATH_PATTERN.matcher(fileName);

    if (gcsPathMatcher.matches()) {
      logger.debug("Got GCS target with bucket '{}'.", gcsPathMatcher.group("bucket"));
      if (StringUtils.endsWithIgnoreCase(fileName, ".zip")) {
        return fileName;
      }
      if (fileName.endsWith("/")) {
        return fileName + defaultFileName;
      }
      return fileName + "/" + defaultFileName;
    } else {
      Path path = Paths.get(fileName);
      boolean isZipFile =
          StringUtils.endsWithIgnoreCase(fileName, ".zip") && !path.toFile().isDirectory();
      if (path.toFile().isFile() && !isZipFile) {
        throw new IllegalStateException(
            String.format(
                "A file already exists at %1$s. If you want to create a directory, please"
                    + " provide the path to the directory. If you want to create %1$s.zip,"
                    + " please add the `.zip` extension manually.",
                fileName));
      }
      return isZipFile ? fileName : path.resolve(defaultFileName).toString();
    }
  }

  private void printTaskResults(SummaryPrinter summaryPrinter, TaskSetState state) {
    summaryPrinter.printSummarySection(
        linePrinter -> {
          linePrinter.println("Task Summary:");
          for (TaskResultSummary item : state.getTaskResultSummaries()) {
            linePrinter.println(item.toString());
          }
        });
  }

  private boolean checkRequiredTaskSuccess(
      SummaryPrinter summaryPrinter, TaskSetState state, String outputFileName) {
    long failedRequiredTasks = state.getFailedRequiredTaskCount();
    if (failedRequiredTasks > 0) {
      summaryPrinter.printSummarySection(
          linePrinter ->
              linePrinter.println("ERROR: %s required task[s] failed.", failedRequiredTasks));
      return false;
    }
    return true;
  }

  private void logFinalSummary(
      SummaryPrinter summaryPrinter,
      TaskSetState state,
      long outputFileLength,
      Stopwatch stopwatch,
      String outputFileLocation,
      boolean requiredTaskSucceeded) {
    summaryPrinter.printSummarySection(
        linePrinter -> {
          linePrinter.println(
              "Dumper wrote " + outputFileLength + " bytes, took " + stopwatch + ".");
          linePrinter.println(
              "Task summary: "
                  + state.getTasksReports().stream()
                      .map(taskReport -> taskReport.count() + " " + taskReport.state())
                      .collect(joining(", ")));
          if (requiredTaskSucceeded) {
            linePrinter.println("Output saved to '%s'", outputFileLocation);
          } else {
            linePrinter.println(
                "Output, including debugging information, saved to '%s'", outputFileLocation);
          }

          String stateToPrint = requiredTaskSucceeded ? "SUCCEEDED" : "FAILED";
          linePrinter.println("Dumper execution: " + stateToPrint);
        });
  }

  @FunctionalInterface
  public interface ShutdownHook {
    void shutdown(String outputzip);
  }
}
