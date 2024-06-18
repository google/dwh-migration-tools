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

import com.google.common.collect.Lists;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.oracle.OracleMetadataConnector.GroupTask;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.util.Arrays;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageTask extends AbstractTask<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(MessageTask.class);

  private final GroupTask<?>[] tasks;

  public MessageTask(@Nonnull GroupTask<?>... ts) {
    super(String.join(", ", Lists.transform(Arrays.asList(ts), GroupTask::getName)));
    tasks = ts;
  }

  // if we are here, means both the dep tasks *have* failed.
  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    LOG.error("All the select tasks failed:");
    int i = 1;
    for (GroupTask<?> task : tasks) {
      LOG.error(
          "({}): {} : {}",
          i++,
          task.getName(),
          ExceptionUtils.getRootCauseMessage(task.getException()));
    }
    return null;
  }

  // This shows up in dry-run
  @Override
  public String toString() {
    return "[ Error if all fail: "
        + String.join(", ", Lists.transform(Arrays.asList(tasks), GroupTask::getName))
        + " ]";
  }
}
