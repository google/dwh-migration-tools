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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

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
  private static final Logger LOG = LoggerFactory.getLogger(BashTask.class);

  private static final Duration SCRIPT_TIMEOUT = Duration.ofMinutes(5);

  private final String scriptName;

  public BashTask(String scriptName) {
    this.scriptName = scriptName;
  }

  @Override
  public String getTargetPath() {
    return scriptName + ".out";
  }

  private void doRun(ByteSink outputSink, ByteSink errorSink, ByteSink exitStatusSink)
      throws IOException, ExecutionException {
    String scriptFilename = scriptName + ".sh";
    Path scriptFile = HadoopScripts.extract(scriptFilename);
    Process process =
        new ProcessBuilder("/bin/bash", scriptFile.toAbsolutePath().toString()).start();
    Future<Void> outputStreamPump;
    Future<Void> errorStreamPump;
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    try (OutputStream outputStream = outputSink.openBufferedStream();
        OutputStream errorStream = errorSink.openBufferedStream()) {
      outputStreamPump =
          executorService.submit(
              throwing(() -> IOUtils.copy(process.getInputStream(), outputStream)));
      errorStreamPump =
          executorService.submit(
              throwing(() -> IOUtils.copy(process.getErrorStream(), errorStream)));
      boolean processFinished = process.waitFor(SCRIPT_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
      LOG.info("Process finished: '{}'", processFinished);
    } catch (InterruptedException e) {
      writeExitStatus(exitStatusSink, "interrupted");
      throw new RuntimeException(e);
    } finally {
      executorService.shutdown();
    }
    writeExitStatusOrTimeout(exitStatusSink, process);
    propagateException(outputStreamPump, "output");
    propagateException(errorStreamPump, "error");
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

  private static void writeExitStatusOrTimeout(ByteSink exitStatusSink, Process process)
      throws IOException {
    try {
      writeExitStatus(exitStatusSink, process.exitValue() + "");
    } catch (Exception e) {
      writeExitStatus(exitStatusSink, "timeout");
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
            context.createOutputHandle(scriptName + ".exit-status"); ) {
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

  private OutputHandle createOutputHandle(TaskRunContext context, String targetPath)
      throws IOException {
    OutputHandle outputHandle = context.newOutputFileHandle(targetPath);
    if (outputHandle.exists()) {
      throw new IllegalStateException(
          String.format("Attempt to create two sinks for the same target path='%s'.", targetPath));
    }
    return outputHandle;
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
