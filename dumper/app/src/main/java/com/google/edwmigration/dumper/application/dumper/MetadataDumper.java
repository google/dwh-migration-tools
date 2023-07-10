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
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.FileSystemOutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.ArgumentsTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcRunSQLScript;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskGroup;
import com.google.edwmigration.dumper.application.dumper.task.TaskResult;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState.Impl;
import com.google.edwmigration.dumper.application.dumper.task.TaskState;
import com.google.edwmigration.dumper.application.dumper.task.VersionTask;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author miguel */
public class MetadataDumper {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataDumper.class);

  private static final ImmutableMap<String, Connector> CONNECTORS;

  static {
    ImmutableMap.Builder<String, Connector> builder = ImmutableMap.builder();
    for (Connector connector : ServiceLoader.load(Connector.class))
      builder.put(connector.getName().toUpperCase(), connector);
    CONNECTORS = builder.build();
  }

  private static final String STARS =
      "*******************************************************************";

  private static final Pattern GCS_PATH_PATTERN =
      Pattern.compile("gs://(?<bucket>[^/]+)/(?<path>.*)");

  private boolean exitOnError = false;

  public MetadataDumper withExitOnError(boolean exitOnError) {
    this.exitOnError = exitOnError;
    return this;
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

  @CheckForNull
  private <T> T runTask(
      @Nonnull TaskRunContext context, @Nonnull TaskSetState.Impl state, Task<T> task)
      throws MetadataDumperUsageException {
    try {
      CHECK:
      {
        TaskState ts = state.getTaskState(task);
        Preconditions.checkState(
            ts == TaskState.NOT_STARTED, "TaskState was bad: " + ts + " for " + task);
      }

      PRECONDITION:
      for (Task.Condition condition : task.getConditions()) {
        if (!condition.evaluate(state)) {
          LOG.debug("Skipped " + task.getName() + " because " + condition.toSkipReason());
          state.setTaskResult(task, TaskState.SKIPPED, null);
          return null;
        }
      }

      RUN:
      {
        T value = task.run(context);
        state.setTaskResult(task, TaskState.SUCCEEDED, value);
        return value;
      }
    } catch (Exception e) {
      // MetadataDumperUsageException should be fatal.
      if (e instanceof MetadataDumperUsageException) throw (MetadataDumperUsageException) e;
      if (e instanceof SQLException && e.getCause() instanceof MetadataDumperUsageException)
        throw (MetadataDumperUsageException) e.getCause();
      // TaskGroup is an attempt to get rid of this condition.
      // We might need an additional TaskRunner / TaskSupport with an overrideable handleException
      // method instead of this runTask() method.
      if (!task.handleException(e)) LOG.warn("Task failed: " + task + ": " + e, e);
      state.setTaskException(task, TaskState.FAILED, e);
      try {
        OutputHandle sink = context.newOutputFileHandle(task.getTargetPath() + ".exception.txt");
        sink.asCharSink(StandardCharsets.UTF_8)
            .writeLines(
                Arrays.asList(
                    task.toString(),
                    "******************************",
                    String.valueOf(new DumperDiagnosticQuery(e).call())));
      } catch (Exception f) {
        LOG.warn("Exception-recorder failed:  " + f, f);
      }
    }
    return null;
  }

  @EnsuresNonNullIf(expression = "#1", result = false)
  private static boolean isNullOrEmpty(@CheckForNull Object[] in) {
    if (in == null) return true;
    return in.length == 0;
  }

  private void print(@Nonnull Task<?> task, int indent) {
    System.out.println(repeat(' ', indent * 2) + task);
    if (task instanceof TaskGroup) {
      for (Task<?> subtask : ((TaskGroup) task).getTasks()) print(subtask, indent + 1);
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
      if (sqlScript != null) tasks.add(new JdbcRunSQLScript(sqlScript));
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

        TaskRunContext runContext =
            new TaskRunContext(sinkFactory, handle, arguments.getThreadPoolSize()) {
              @Override
              public TaskState getTaskState(Task<?> task) {
                return state.getTaskState(task);
              }

              @Override
              public <T> T runChildTask(Task<T> task) throws MetadataDumperUsageException {
                return runTask(this, state, task);
              }
            };
        TASK:
        for (Task<?> task : tasks) {
          runTask(runContext, state, task);
        }

      } finally {
        // We must do this in finally after the ZipFileSystem has been closed.
        File outputFile = new File(outputFileLocation);
        if (outputFile.isFile()) {
          LOG.debug("Dumper wrote " + outputFile.length() + " bytes.");
        }
        LOG.debug("Dumper took " + stopwatch + ".");
      }

      printTaskResults(state);
      printDumperSummary(connector, outputFileLocation);
      checkRequiredTaskSuccess(state, outputFileLocation);
      logStatusSummary(state);
      System.out.println(STARS);
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

  private void printDumperSummary(Connector connector, String outputFileName) {
    if (connector instanceof MetadataConnector) {
      log("Metadata has been saved to " + outputFileName);
    } else if (connector instanceof LogsConnector) {
      log("Logs have been saved to " + outputFileName);
    }
  }

  private void printTaskResults(TaskSetState.Impl state) {
    List<String> lines = new ArrayList<>();
    lines.add("Task Summary:");
    state
        .getTaskResultMap()
        .forEach(
            (task, result) ->
                lines.add(
                    String.format(
                        "Task %s (%s) %s%s",
                        result.getState(),
                        task.getCategory(),
                        task,
                        (result.getException() == null)
                            ? ""
                            : String.format(": %s", result.getException()))));
    log(lines);
  }

  private void checkRequiredTaskSuccess(Impl state, String outputFileName) {
    long requiredTasksNotSucceeded =
        state.getTaskResultMap().entrySet().stream()
            .filter(e -> TaskCategory.REQUIRED.equals(e.getKey().getCategory()))
            .filter(e -> TaskState.FAILED.equals(e.getValue().getState()))
            .count();
    if (requiredTasksNotSucceeded > 0) {
      log(
          "ERROR: " + requiredTasksNotSucceeded + " required task[s] failed.",
          "Output, including debugging information, has been saved to " + outputFileName);
      if (exitOnError) {
        System.out.println(STARS);
        System.exit(1);
      }
    }
  }

  private void logStatusSummary(TaskSetState.Impl state) {
    List<String> lines = new ArrayList<>();
    state.getTaskResultMap().values().stream()
        .map(TaskResult::getState)
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
        .forEach((key, value) -> lines.add(String.format("%d TASKS %s", value, key)));
    log(lines);
  }

  private void log(String... lines) {
    log(Arrays.asList(lines));
  }

  private void log(List<String> lines) {
    System.out.println(STARS);
    lines.stream().map(s -> "* " + s).forEach(System.out::println);
  }
}
