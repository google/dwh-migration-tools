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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import java.io.IOException;
import java.util.Collections;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TasksRunnerTest {
  @Rule public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Before
  public void setUp() {
    System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
  }

  @Test
  public void testCreateContext_returnsValidTaskRunContext() throws IOException {
    OutputHandleFactory mockSinkFactory = mock(OutputHandleFactory.class);
    Handle mockHandle = mock(Handle.class);
    int threadPoolSize = 2;
    TaskSetState.Impl mockState = mock(TaskSetState.Impl.class);
    ConnectorArguments arguments = new ConnectorArguments("--connector", "test");
    TasksRunner runner =
        new TasksRunner(
            mockSinkFactory,
            mockHandle,
            threadPoolSize,
            mockState,
            Collections.emptyList(),
            arguments);

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
