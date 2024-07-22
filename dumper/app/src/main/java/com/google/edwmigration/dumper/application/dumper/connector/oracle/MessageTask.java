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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task to print error messages after all queries of a certain type failed. */
class MessageTask extends AbstractTask<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(MessageTask.class);

  private final GroupTask[] tasks;
  /** */
  private final String names;

  private MessageTask(GroupTask[] tasks, String names) {
    super(names);
    this.tasks = tasks;
    this.names = names;
  }

  static MessageTask create(@Nonnull GroupTask... ts) {
    GroupTask[] tasks = ts.clone();
    String names = toNames(ts);
    return new MessageTask(tasks, names);
  }

  Iterable<String> getMessages() {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    builder.add("All the select tasks failed:");
    int i = 1;
    for (GroupTask task : tasks) {
      String cause = ExceptionUtils.getRootCauseMessage(task.getException());
      String message = String.format("(%d): %s : %s", i, task.getName(), cause);
      builder.add(message);
      i++;
    }
    return builder.build();
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle) {
    getMessages().forEach(LOG::error);
    return null;
  }

  /**
   * Returns a string representation of the object. Used in dry-run.
   *
   * @return The string representation of this class, including a list of dependency Tasks.
   */
  @Override
  public String toString() {
    return "[ Error if all fail: " + names + " ]";
  }

  private static String toNames(GroupTask... tasks) {
    return stream(tasks).map(GroupTask::getName).collect(joining(", "));
  }
}
