/*
 * Copyright 2022-2024 Google LLC
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
import com.google.edwmigration.dumper.application.dumper.SummaryPrinter.SummaryLinePrinter;
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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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

  private static final Logger LOG = LoggerFactory.getLogger(MetadataDumper.class);

  private static final Pattern GCS_PATH_PATTERN =
      Pattern.compile("gs://(?<bucket>[^/]+)/(?<path>.*)");

  private static DateTimeFormatter OUTPUT_DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(ZoneOffset.UTC);

  public boolean run(String... args) throws Exception {
    ConnectorArguments arguments = new ConnectorArguments(JsonResponseFile.addResponseFiles(args));
    try {
      return run(arguments);
    } finally {
      if (arguments.saveResponseFile()) {
        JsonResponseFile.save(arguments);
      }
    }
  }

  public boolean run(@Nonnull ConnectorArguments arguments) throws Exception {
    String connectorName = arguments.getConnectorName();
    if (connectorName == null) {
      LOG.error("Target DBMS is required");
      return false;
    }

    Connector connector = ConnectorRepository.getInstance().getByName(connectorName);
    if (connector == null) {
      LOG.error(
          "Target DBMS '{}' not supported; available are {}.",
          connectorName,
          ConnectorRepository.getInstance().getAllNames());
      return false;
    }
    return run(connector, arguments);
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
      LOG.debug(
          String.format(
              "Setting up CloudStorageFileSystem with bucket '%s' and path '%s'.", bucket, path));
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

  protected boolean run(@Nonnull Connector connector, @Nonnull ConnectorArguments arguments)
      throws Exception {
    List<Task<?>> tasks = new ArrayList<>();
    tasks.add(new VersionTask());
    tasks.add(new ArgumentsTask(arguments));
    {
      File sqlScript = arguments.getSqlScript();
      if (sqlScript != null) {
        tasks.add(new JdbcRunSQLScript(sqlScript));
      }
    }
    connector.addTasksTo(tasks, arguments);

    // The default output file is based on the connector.
    // We had a customer request to base it on the database, but that isn't well-defined,
    // as there may be 0 or N databases in a single file.
    String outputFileLocation = getOutputFileLocation(connector, arguments);

    if (arguments.isDryRun()) {
      String title = "Dry run: Printing task list for " + connector.getName();
      System.out.println(title);
      System.out.println(repeat('=', title.length()));
      System.out.println("Writing to " + outputFileLocation);
      for (Task<?> task : tasks) {
        print(task, 1);
      }
      return true;
    } else {
      Stopwatch stopwatch = Stopwatch.createStarted();
      long outputFileLength = 0;
      TaskSetState.Impl state = new TaskSetState.Impl();

      LOG.info("Using " + connector);
      SummaryPrinter summaryPrinter = new SummaryPrinter();
      try (Closer closer = Closer.create()) {
        Path outputPath = prepareOutputPath(outputFileLocation, closer, arguments);

        URI outputUri = URI.create("jar:" + outputPath.toUri());
        // LOG.debug("Is a zip file: " + outputUri);
        Map<String, Object> fileSystemProperties =
            ImmutableMap.<String, Object>builder()
                .put("create", "true")
                .put("useTempFile", Boolean.TRUE)
                .build();
        FileSystem fileSystem =
            closer.register(FileSystems.newFileSystem(outputUri, fileSystemProperties));
        OutputHandleFactory sinkFactory =
            new FileSystemOutputHandleFactory(fileSystem, "/"); // It's required to be "/"
        LOG.debug("Target filesystem is " + sinkFactory);

        Handle handle = closer.register(connector.open(arguments));

        new TasksRunner(sinkFactory, handle, arguments.getThreadPoolSize(), state, tasks, arguments)
            .run();
      } finally {
        // We must do this in finally after the ZipFileSystem has been closed.
        File outputFile = new File(outputFileLocation);
        if (outputFile.isFile()) {
          outputFileLength = outputFile.length();
        }
      }

      printTaskResults(summaryPrinter, state);
      boolean requiredTaskSucceeded =
          checkRequiredTaskSuccess(summaryPrinter, state, outputFileLocation);
      logFinalSummary(
          summaryPrinter,
          state,
          connector,
          outputFileLength,
          stopwatch,
          outputFileLocation,
          requiredTaskSucceeded);
      return requiredTaskSucceeded;
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
        .orElseGet(() -> defaultFileName);
  }

  private String getVerifiedFile(String defaultFileName, String fileName) {
    Matcher gcsPathMatcher = GCS_PATH_PATTERN.matcher(fileName);

    if (gcsPathMatcher.matches()) {
      LOG.debug("Got GCS target with bucket '{}'.", gcsPathMatcher.group("bucket"));
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
          linePrinter -> {
            linePrinter.println("ERROR: %s required task[s] failed.", failedRequiredTasks);
          });
      return false;
    }
    return true;
  }

  private void outputFirstAndLastQueryLogEnries(SummaryLinePrinter linePrinter) {

    if (QueryLogSharedState.queryLogEntries.size() == 0) {
      return;
    }

    linePrinter.println(
        "The first query log entry is '%s' UTC and the last query log entry is '%s' UTC",
        QueryLogSharedState.queryLogEntries
            .get(QueryLogSharedState.QueryLogEntry.QUERY_LOG_FIRST_ENTRY)
            .format(OUTPUT_DATE_FORMAT),
        QueryLogSharedState.queryLogEntries
            .get(QueryLogSharedState.QueryLogEntry.QUERY_LOG_LAST_ENTRY)
            .format(OUTPUT_DATE_FORMAT));
  }

  private void logFinalSummary(
      SummaryPrinter summaryPrinter,
      TaskSetState state,
      Connector connector,
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
          // For now, it will return true only for TeradataLogsConnector and Terada14LogsConnector
          if (connector.isLogConnector()) {
            outputFirstAndLastQueryLogEnries(linePrinter);
          }
          if (requiredTaskSucceeded) {
            linePrinter.println("Output saved to '%s'", outputFileLocation);
          } else {
            linePrinter.println(
                "Output, including debugging information, saved to '%s'", outputFileLocation);
          }
        });
  }
}
