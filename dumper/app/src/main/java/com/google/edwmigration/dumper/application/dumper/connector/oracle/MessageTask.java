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

import static org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList.builderWithExpectedSize;

import com.google.common.collect.Lists;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.oracle.OracleMetadataConnector.GroupTask;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MessageTask extends AbstractTask<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(MessageTask.class);

  private final GroupTask<?>[] tasks;
  private final String names;

  private MessageTask(GroupTask<?>[] tasks, String names) {
    super(names);
    this.tasks = tasks;
    this.names = names;
  }

  static MessageTask create(@Nonnull GroupTask<?>... ts) {
    GroupTask<?>[] tasks = ts.clone();
    String names = toNames(ts);
    return new MessageTask(tasks, names);
  }

  Iterable<String> getMessages() {
    int size = 1 + tasks.length;
    ImmutableList.Builder<String> builder = builderWithExpectedSize(size);
    builder.add("All the select tasks failed:");
    int i = 1;
    for (GroupTask<?> task : tasks) {
      String cause = ExceptionUtils.getRootCauseMessage(task.getException());
      String message = String.format("(%d): %s : %s", i, task.getName(), cause);
      builder.add(message);
      i++;
    }
    return builder.build();
  }

  // if we are here, means both the dep tasks *have* failed.
  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle) {
    getMessages().forEach(LOG::error);
    return null;
  }

  // This shows up in dry-run
  @Override
  public String toString() {
    return "[ Error if all fail: " + names + " ]";
  }

  private static String toNames(GroupTask<?>... tasks) {
    List<String> tokens = Lists.transform(Arrays.asList(tasks), GroupTask::getName);
    return String.join(", ", tokens);
  }
}
