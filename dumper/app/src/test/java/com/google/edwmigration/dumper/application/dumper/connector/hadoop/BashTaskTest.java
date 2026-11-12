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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.edwmigration.dumper.application.dumper.io.FileSystemOutputHandle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BashTaskTest {

  private static final Duration SHORT_TIMEOUT = Duration.ofSeconds(2);

  private static final long PROCESS_EXIT_WAIT_MILLIS = 10_000;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Path outputDir;

  private TaskRunContext context;

  @Before
  public void setUp() throws IOException {
    outputDir = temporaryFolder.newFolder("output").toPath();
    context = mock(TaskRunContext.class);
    when(context.createOutputHandle(anyString()))
        .thenAnswer(invocation -> new FileSystemOutputHandle(outputDir, invocation.getArgument(0)));
  }

  @Test
  public void run_scriptFinishes_writesOutputAndExitStatus() throws Exception {
    Path script = writeScript("echo 'hello'\necho 'oops' >&2\nexit 3\n");

    new BashTask("test-script", script, SHORT_TIMEOUT).run(context);

    assertEquals("hello\n", readOutput("test-script.out"));
    assertEquals("oops\n", readOutput("test-script.err"));
    assertEquals("3", readOutput("test-script.exit-status"));
  }

  @Test(timeout = 30_000)
  public void run_scriptExceedsTimeout_killsScriptAndItsChildren() throws Exception {
    Path scriptPidFile = temporaryFolder.getRoot().toPath().resolve("script.pid");
    Path childPidFile = temporaryFolder.getRoot().toPath().resolve("child.pid");
    Path script =
        writeScript(
            String.format(
                "sleep 120 &\necho $! > '%s'\necho $$ > '%s'\nsleep 120\n",
                childPidFile, scriptPidFile));

    new BashTask("slow-script", script, SHORT_TIMEOUT).run(context);

    assertEquals("timeout", readOutput("slow-script.exit-status"));
    assertFalse("Script is still running.", waitUntilNotRunning(readPid(scriptPidFile)));
    assertFalse(
        "Child of the script is still running.", waitUntilNotRunning(readPid(childPidFile)));
  }

  private Path writeScript(String body) throws IOException {
    Path script = temporaryFolder.newFile().toPath();
    Files.write(script, ("#!/bin/bash\n\n" + body).getBytes(UTF_8));
    return script;
  }

  private String readOutput(String name) throws IOException {
    return new String(Files.readAllBytes(outputDir.resolve(name)), UTF_8);
  }

  private static String readPid(Path pidFile) throws IOException {
    return new String(Files.readAllBytes(pidFile), UTF_8).trim();
  }

  /** Returns whether the process is still running after the wait. */
  private static boolean waitUntilNotRunning(String pid) throws Exception {
    long deadline = System.currentTimeMillis() + PROCESS_EXIT_WAIT_MILLIS;
    while (isRunning(pid)) {
      if (System.currentTimeMillis() > deadline) {
        return true;
      }

      TimeUnit.MILLISECONDS.sleep(100);
    }

    return false;
  }

  private static boolean isRunning(String pid) throws Exception {
    Process kill = new ProcessBuilder("kill", "-0", pid).start();
    return kill.waitFor() == 0;
  }
}
