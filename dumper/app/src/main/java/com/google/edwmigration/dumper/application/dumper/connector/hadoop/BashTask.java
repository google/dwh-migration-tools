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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BashTask implements Task<Void> {
  private static final Logger logger = LoggerFactory.getLogger(BashTask.class);

  private static final Duration SCRIPT_TIMEOUT = Duration.ofMinutes(1);

  private static final Duration STOP_TIMEOUT = Duration.ofSeconds(5);

  /**
   * Runs the script given as the first argument in its own process group, so that the script and
   * every process it starts can be stopped together; Java 8 cannot list the children of a process.
   * On SIGTERM, SIGINT or SIGHUP the wrapper kills that process group. Otherwise it exits with the
   * exit status of the script.
   */
  private static final String PROCESS_GROUP_WRAPPER =
      "trap 'kill -KILL -- -$script_pid 2>/dev/null; exit 143' TERM INT HUP; "
          + "set -m; /bin/bash \"$1\" & script_pid=$!; wait $script_pid";

  private final String scriptName;

  private final Path scriptFile;

  private final Duration scriptTimeout;

  public BashTask(String scriptName, Path scriptFile) {
    this(scriptName, scriptFile, SCRIPT_TIMEOUT);
  }

  @VisibleForTesting
  BashTask(String scriptName, Path scriptFile, Duration scriptTimeout) {
    this.scriptName = scriptName;
    this.scriptFile = scriptFile;
    this.scriptTimeout = scriptTimeout;
  }

  @Override
  public String getTargetPath() {
    return scriptName + ".out";
  }

  private void doRun(ByteSink outputSink, ByteSink errorSink, ByteSink exitStatusSink)
      throws IOException, ExecutionException {
    logger.info("Running bash task '{}'", scriptName);
    Process process =
        new ProcessBuilder(
                "/bin/bash",
                "-c",
                PROCESS_GROUP_WRAPPER,
                "bash",
                scriptFile.toAbsolutePath().toString())
            .start();
    boolean processFinished;
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    try (OutputStream outputStream = outputSink.openBufferedStream();
        OutputStream errorStream = errorSink.openBufferedStream()) {
      Future<Void> outputStreamPump =
          executorService.submit(
              throwing(() -> IOUtils.copy(process.getInputStream(), outputStream)));
      Future<Void> errorStreamPump =
          executorService.submit(
              throwing(() -> IOUtils.copy(process.getErrorStream(), errorStream)));
      processFinished = process.waitFor(scriptTimeout.toMillis(), TimeUnit.MILLISECONDS);
      logger.info("Process finished: '{}'", processFinished);
      if (!processFinished) {
        logger.warn("Bash task '{}' timed out after {}, stopping it.", scriptName, scriptTimeout);
        stop(process);
      }

      propagateException(outputStreamPump, "output");
      propagateException(errorStreamPump, "error");
    } catch (InterruptedException e) {
      stop(process);
      writeExitStatus(exitStatusSink, "interrupted");
      throw new RuntimeException(e);
    } finally {
      executorService.shutdown();
    }
    writeExitStatus(
        exitStatusSink, processFinished ? String.valueOf(process.exitValue()) : "timeout");
  }

  /**
   * Stops the script and the processes it started. The wrapper reacts to SIGTERM by killing the
   * process group of the script. If the wrapper does not exit in time, it is killed directly.
   */
  private static void stop(Process process) {
    process.destroy();
    try {
      if (!process.waitFor(STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
        process.destroyForcibly();
      }
    } catch (InterruptedException e) {
      process.destroyForcibly();
      Thread.currentThread().interrupt();
    }
  }

  private void propagateException(Future<Void> future, String streamName)
      throws ExecutionException {
    try {
      future.get();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          String.format(
              "Error while processing the '%s' stream from the bash command.", streamName),
          ex);
    }
  }

  private static void writeExitStatus(ByteSink exitStatusSink, String value) throws IOException {
    try (Writer wr = exitStatusSink.asCharSink(UTF_8).openBufferedStream()) {
      wr.write(value);
    }
  }

  @Override
  public Void run(TaskRunContext context) throws Exception {
    try (OutputHandle outputHandle = context.createOutputHandle(scriptName + ".out");
        OutputHandle errorOutputHandle = context.createOutputHandle(scriptName + ".err");
        OutputHandle exitStatusOutputHandle =
            context.createOutputHandle(scriptName + ".exit-status")) {
      doRun(
          outputHandle.asTemporaryByteSink(),
          errorOutputHandle.asTemporaryByteSink(),
          exitStatusOutputHandle.asTemporaryByteSink());
    }
    return null;
  }

  @Override
  public TaskCategory getCategory() {
    return TaskCategory.OPTIONAL;
  }

  @Override
  public String toString() {
    return format("Bash script execution '%s'", scriptName);
  }

  private static Callable<Void> throwing(ThrowingRunnable runnable) {
    return () -> {
      runnable.run();
      return null;
    };
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
