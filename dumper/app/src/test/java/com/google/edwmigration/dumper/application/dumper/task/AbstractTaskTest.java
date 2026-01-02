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
package com.google.edwmigration.dumper.application.dumper.task;

import static com.google.edwmigration.dumper.application.dumper.io.OutputHandle.WriteMode.APPEND_EXISTING;
import static com.google.edwmigration.dumper.application.dumper.io.OutputHandle.WriteMode.CREATE_TRUNCATE;
import static com.google.edwmigration.dumper.application.dumper.task.AbstractTask.TargetInitialization.CREATE;
import static com.google.edwmigration.dumper.application.dumper.task.AbstractTask.TargetInitialization.DO_NOT_CREATE;
import static com.google.edwmigration.dumper.application.dumper.task.TaskCategory.REQUIRED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle.WriteMode;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask.DummyByteSink;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask.SinkWrapper;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask.TargetInitialization;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask.TaskOptions;
import java.io.IOException;
import org.junit.Test;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/** @author shevek */
@RunWith(Theories.class)
public abstract class AbstractTaskTest {

  @Test
  public void getWrapper_createEnabledAndModeIsAppend_returnsAppendWrapper() throws IOException {
    TaskRunContext mockContext = mock(TaskRunContext.class);
    OutputHandle handle = mock(OutputHandle.class);
    when(mockContext.newOutputFileHandle(anyString())).thenReturn(handle);
    when(handle.exists()).thenReturn(false);
    AbstractTask<?> task = testTask(CREATE, APPEND_EXISTING);

    SinkWrapper wrapper = task.getWrapper(mockContext);

    verify(handle).asByteSink(any());
    assertSame(handle, wrapper.handle);
    assertFalse(wrapper.shouldCommit);
  }

  @Test
  public void getWrapper_createEnabledAndModeIsCreate_returnsAppendWrapper() throws IOException {
    TaskRunContext mockContext = mock(TaskRunContext.class);
    OutputHandle handle = mock(OutputHandle.class);
    when(mockContext.newOutputFileHandle(anyString())).thenReturn(handle);
    when(handle.exists()).thenReturn(false);
    AbstractTask<?> task = testTask(CREATE, CREATE_TRUNCATE);

    SinkWrapper wrapper = task.getWrapper(mockContext);

    verify(handle).asTemporaryByteSink(any());
    assertSame(handle, wrapper.handle);
    assertTrue(wrapper.shouldCommit);
  }

  @Theory
  public void getWrapper_createEnabledAndHandleExists_returnsSkipWrapper(WriteMode mode)
      throws IOException {
    TaskRunContext mockContext = mock(TaskRunContext.class);
    OutputHandle handle = mock(OutputHandle.class);
    when(mockContext.newOutputFileHandle(anyString())).thenReturn(handle);
    when(handle.exists()).thenReturn(true);
    AbstractTask<?> task = testTask(CREATE, mode);

    SinkWrapper wrapper = task.getWrapper(mockContext);

    assertNull(wrapper.sink);
    assertSame(handle, wrapper.handle);
    assertFalse(wrapper.shouldCommit);
  }

  @Theory
  public void getWrapper_createNotEnabled_returnsDecoyWrapper(WriteMode mode) throws IOException {
    AbstractTask<?> task = testTask(DO_NOT_CREATE, mode);

    SinkWrapper wrapper = task.getWrapper(mock(TaskRunContext.class));

    assertEquals(DummyByteSink.INSTANCE, wrapper);
    assertNull(wrapper.handle);
    assertFalse(wrapper.shouldCommit);
  }

  static AbstractTask<?> testTask(TargetInitialization strategy, OutputHandle.WriteMode mode) {
    TaskOptions options =
        TaskOptions.builder().setTargetInitialization(strategy).setWriteMode(mode).build();
    return new JdbcSelectTask("test-file", "SELECT 1", REQUIRED, options);
  }
}
