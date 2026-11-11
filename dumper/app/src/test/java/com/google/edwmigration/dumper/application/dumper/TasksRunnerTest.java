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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.FileSystemOutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetStateCollector;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetStateImpl;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TasksRunnerTest {
  @Rule public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
  }

  @Test
  public void run_taskFails_exceptionFileOmitsCommandLineSecrets() throws Exception {
    String secret = "S3cr3t-Value-For-Test";
    System.setProperty("sun.java.command", "Main --connector test --password " + secret);
    Path outputRoot = temporaryFolder.getRoot().toPath();
    ConnectorArguments arguments = new ConnectorArguments("--connector", "test");
    TasksRunner runner =
        new TasksRunner(
            new FileSystemOutputHandleFactory(outputRoot),
            mock(Handle.class),
            new TaskSetStateImpl(),
            ImmutableList.of(new FailingTask()),
            arguments);

    runner.run();

    Path exceptionFile = outputRoot.resolve(FailingTask.TARGET_PATH + ".exception.txt");
    String report = new String(Files.readAllBytes(exceptionFile), UTF_8);
    assertTrue(report.contains(FailingTask.MESSAGE));
    assertFalse(report.contains(secret));
  }

  private static class FailingTask implements Task<Void> {

    static final String TARGET_PATH = "failing.csv";
    static final String MESSAGE = "Simulated task failure";

    @Override
    public String getTargetPath() {
      return TARGET_PATH;
    }

    @Override
    public Void run(TaskRunContext context) {
      throw new IllegalStateException(MESSAGE);
    }

    @Override
    public boolean handleException(Exception e) {
      // Keeps the expected failure out of the test log.
      return true;
    }
  }

  @Test
  public void testCreateContext_returnsValidTaskRunContext() throws IOException {
    OutputHandleFactory mockSinkFactory = mock(OutputHandleFactory.class);
    Handle mockHandle = mock(Handle.class);
    int threadPoolSize = 2;
    TaskSetStateCollector mockState = mock(TaskSetStateImpl.class);
    ConnectorArguments arguments = new ConnectorArguments("--connector", "test");
    TasksRunner runner =
        new TasksRunner(mockSinkFactory, mockHandle, mockState, Collections.emptyList(), arguments);

    try {
      java.lang.reflect.Method method =
          TasksRunner.class.getDeclaredMethod(
              "createContext",
              OutputHandleFactory.class,
              Handle.class,
              int.class,
              ConnectorArguments.class);
      method.setAccessible(true);
      Object context =
          method.invoke(runner, mockSinkFactory, mockHandle, threadPoolSize, arguments);

      assertNotNull(context);
      assertTrue(context instanceof TaskRunContext);

      TaskRunContext taskRunContext = (TaskRunContext) context;
      assertEquals(mockHandle, taskRunContext.getHandle());
      assertEquals(arguments, taskRunContext.getArguments());
    } catch (Exception e) {
      fail("Reflection failed: " + e.getMessage());
    }
  }
}
