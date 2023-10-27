/*
 * Copyright 2022-2023 Google LLC
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

import static org.apache.commons.lang3.StringUtils.repeat;

import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.FileSystemOutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.ArgumentsTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcRunSQLScript;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskGroup;
import com.google.edwmigration.dumper.application.dumper.task.TaskResult;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState.Impl;
import com.google.edwmigration.dumper.application.dumper.task.TaskState;
import com.google.edwmigration.dumper.application.dumper.task.VersionTask;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author miguel */
public class MetadataDumper {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataDumper.class);

  private static final ImmutableMap<String, Connector> CONNECTORS;

  static {
    ImmutableMap.Builder<String, Connector> builder = ImmutableMap.builder();
    for (Connector connector : ServiceLoader.load(Connector.class)) {
      builder.put(connector.getName().toUpperCase(), connector);
    }
    CONNECTORS = builder.build();
  }

  private static final Pattern GCS_PATH_PATTERN =
      Pattern.compile("gs://(?<bucket>[^/]+)/(?<path>.*)");

  private boolean exitOnError = false;

  public MetadataDumper withExitOnError(boolean exitOnError) {
    this.exitOnError = exitOnError;
    return this;
  }

  public void run(String... args) throws Exception {
    ConnectorArguments arguments = new ConnectorArguments(JsonResponseFile.addResponseFiles(args));
    try {
      run(arguments);
    } finally {
      if (arguments.saveResponseFile()) {
        JsonResponseFile.save(arguments);
      }
    }
  }

  public void run(@Nonnull ConnectorArguments arguments) throws Exception {
    String connectorName = arguments.getConnectorName();
    if (connectorName == null) {
      LOG.error("Target DBMS is required");
      return;
    }

    Connector connector = CONNECTORS.get(connectorName.toUpperCase());
    if (connector == null) {
      LOG.error(
          "Target DBMS "
              + connectorName
              + " not supported; available are "
              + CONNECTORS.keySet()
              + ".");
      return;
    }
    run(connector, arguments);
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

  protected void run(@Nonnull Connector connector, @Nonnull ConnectorArguments arguments)
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
    } else {
      Stopwatch stopwatch = Stopwatch.createStarted();
      TaskSetState.Impl state = new TaskSetState.Impl();

      LOG.info("Using " + connector);
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

        new TasksRunner(sinkFactory, handle, arguments.getThreadPoolSize(), state, tasks).run();
      } finally {
        // We must do this in finally after the ZipFileSystem has been closed.
        File outputFile = new File(outputFileLocation);
        if (outputFile.isFile()) {
          LOG.debug("Dumper wrote " + outputFile.length() + " bytes.");
        }
        LOG.debug("Dumper took " + stopwatch + ".");
      }

      SummaryPrinter summaryPrinter = new SummaryPrinter();
      printTaskResults(summaryPrinter, state);
      printDumperSummary(summaryPrinter, connector, outputFileLocation);
      checkRequiredTaskSuccess(summaryPrinter, state, outputFileLocation);
      logStatusSummary(summaryPrinter, state);
    }
  }

  private String getOutputFileLocation(Connector connector, ConnectorArguments arguments) {
    // The default output file is based on the connector.
    // We had a customer request to base it on the database, but that isn't well-defined,
    // as there may be 0 or N databases in a single file.
    String defaultFileName = connector.getDefaultFileName(arguments.isAssessment());
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

  private void printDumperSummary(
      SummaryPrinter summaryPrinter, Connector connector, String outputFileName) {
    if (connector instanceof MetadataConnector) {
      summaryPrinter.printSummarySection("Metadata has been saved to " + outputFileName);
    } else if (connector instanceof LogsConnector) {
      summaryPrinter.printSummarySection("Logs have been saved to " + outputFileName);
    }
  }

  private void printTaskResults(SummaryPrinter summaryPrinter, TaskSetState.Impl state) {
    summaryPrinter.printSummarySection(
        linePrinter -> {
          linePrinter.println("Task Summary:");
          state
              .getTaskResultMap()
              .forEach(
                  (task, result) ->
                      linePrinter.println(
                          "Task %s (%s) %s%s",
                          result.getState(),
                          task.getCategory(),
                          task,
                          (result.getException() == null)
                              ? ""
                              : String.format(": %s", result.getException())));
        });
  }

  private void checkRequiredTaskSuccess(
      SummaryPrinter summaryPrinter, Impl state, String outputFileName) {
    long requiredTasksNotSucceeded =
        state.getTaskResultMap().entrySet().stream()
            .filter(e -> TaskCategory.REQUIRED.equals(e.getKey().getCategory()))
            .filter(e -> TaskState.FAILED.equals(e.getValue().getState()))
            .count();
    if (requiredTasksNotSucceeded > 0) {
      summaryPrinter.printSummarySection(
          linePrinter -> {
            linePrinter.println("ERROR: %s required task[s] failed.", requiredTasksNotSucceeded);
            linePrinter.println(
                "Output, including debugging information, has been saved to '%s'.", outputFileName);
          });
      if (exitOnError) {
        System.exit(1);
      }
    }
  }

  private void logStatusSummary(SummaryPrinter summaryPrinter, TaskSetState.Impl state) {
    summaryPrinter.printSummarySection(
        linePrinter -> {
          state.getTaskResultMap().values().stream()
              .map(TaskResult::getState)
              .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
              .forEach((key, value) -> linePrinter.println("%d TASKS %s", value, key));
        });
  }
}
