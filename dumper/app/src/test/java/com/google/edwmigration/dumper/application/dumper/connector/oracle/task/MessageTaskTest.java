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
package com.google.edwmigration.dumper.application.dumper.connector.oracle.task;

import static com.google.common.collect.ImmutableList.copyOf;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.oracle.task.GroupTask;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MessageTaskTest {

  @Test
  public void getMessages_success() {
    ImmutableList<String> messages =
        ImmutableList.of(
            "All the select tasks failed:",
            "(1): fake : RuntimeException: ExceptionCauseA",
            "(2): fake : RuntimeException: ExceptionCauseB");
    GroupTask subtaskA = new FakeTask("ExceptionCauseA");
    GroupTask subtaskB = new FakeTask("ExceptionCauseB");
    MessageTask task = MessageTask.create(subtaskA, subtaskB);

    // Act
    Iterable<String> taskMessages = task.getMessages();

    // Assert
    assertEquals(messages, copyOf(taskMessages));
  }

  @Test
  public void toString_success() {
    GroupTask subtaskA = new FakeTask("ExceptionCauseA");
    GroupTask subtaskB = new FakeTask("ExceptionCauseB");
    MessageTask task = MessageTask.create(subtaskA, subtaskB);

    // Act
    String taskString = task.toString();

    // Assert
    assertEquals("[ Error if all fail: fake, fake ]", taskString);
  }

  private static class FakeTask extends AbstractTask<Summary> implements GroupTask {
    private final Exception exception;

    FakeTask(String message) {
      super("fake");
      exception = new RuntimeException(message);
    }

    @Override
    protected Summary doRun(
        @CheckForNull TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle) {
      return Summary.EMPTY;
    }

    @Override
    public Exception getException() {
      return exception;
    }
  }
}
