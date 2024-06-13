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
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URL;
import java.time.Duration;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;

public class BashTask implements Task<Void> {

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
      throws IOException {
    String scriptFilename = scriptName + ".sh";
    URL resourceUrl = Resources.getResource("hadoop-scripts/" + scriptFilename);
    byte[] scriptBody = Resources.toByteArray(resourceUrl);
    File scriptDir = new File("dwh-migration-tools-tmp");
    scriptDir.mkdirs();
    File scriptFile = new File(scriptDir, scriptFilename);
    Files.write(scriptBody, scriptFile);
    scriptFile.setExecutable(true);
    CommandLine cmdLine = CommandLine.parse("/bin/bash " + scriptFile.getAbsolutePath());
    DefaultExecutor executor = DefaultExecutor.builder().get();
    executor.setExitValue(1);
    ExecuteWatchdog watchdog = ExecuteWatchdog.builder().setTimeout(SCRIPT_TIMEOUT).get();
    executor.setWatchdog(watchdog);
    try (OutputStream outputStream = outputSink.openBufferedStream();
        OutputStream errorStream = errorSink.openBufferedStream()) {
      PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream, errorStream);
      executor.setStreamHandler(streamHandler);
      int exitValue = executor.execute(cmdLine);
      try (Writer wr = exitStatusSink.asCharSink(UTF_8).openBufferedStream()) {
        wr.write("" + exitValue);
      }
    } catch (ExecuteException e) {
      try (Writer wr = exitStatusSink.asCharSink(UTF_8).openBufferedStream()) {
        wr.write("" + e.getExitValue());
      }
      if (e.getExitValue() != 0) {
        throw e;
      }
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
}
