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

import static org.junit.Assert.assertEquals;

import com.google.common.base.Charsets;
import com.google.common.io.ByteSink;
import com.google.common.io.Files;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.FileSystemOutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle.WriteMode;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask.TaskOptions;
import com.google.edwmigration.dumper.application.dumper.test.DummyTaskRunContextFactory;
import java.io.File;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AppendTaskTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testAppend() throws Exception {
    File outputFile = new File(tempFolder.getRoot(), "append_test.csv");
    FileSystemOutputHandleFactory sinkFactory =
        new FileSystemOutputHandleFactory(tempFolder.getRoot().toPath());
    Handle handle = () -> {};

    // Task 1: Create
    TaskRunContext context1 =
        DummyTaskRunContextFactory.create(sinkFactory, handle, new ConnectorArguments());
    new AbstractTask<Void>("append_test.csv") {
      @Override
      protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
          throws Exception {
        sink.asCharSink(StandardCharsets.UTF_8).write("Line 1\n");
        return null;
      }
    }.run(context1);

    // Task 2: Append
    TaskRunContext context2 =
        DummyTaskRunContextFactory.create(sinkFactory, handle, new ConnectorArguments());
    new AbstractTask<Void>(
        "append_test.csv", TaskOptions.DEFAULT.withWriteMode(WriteMode.APPEND_EXISTING)) {
      @Override
      protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
          throws Exception {
        sink.asCharSink(StandardCharsets.UTF_8).write("Line 2\n");
        return null;
      }
    }.run(context2);

    String content = Files.asCharSource(outputFile, Charsets.UTF_8).read();
    assertEquals("Line 1\nLine 2\n", content);
  }
}
