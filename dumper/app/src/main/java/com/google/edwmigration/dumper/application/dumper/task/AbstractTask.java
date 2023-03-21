/*
 * Copyright 2022-2023 Google LLC
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

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
public abstract class AbstractTask<T> implements Task<T> {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTask.class);

  public static final CSVFormat FORMAT =
      CSVFormat.DEFAULT
          .withRecordSeparator("\n")
          .withEscape('\\')
          // Trimming whitespace within quotes makes impossible to rebuild queries split on multiple
          // records
          // false is the default, but we choose to emphasize it here.
          .withTrim(false)
          // This ignores whitespace OUTSIDE quotes when lexing.
          .withIgnoreSurroundingSpaces()
          .withQuoteMode(QuoteMode.MINIMAL);

  private final String targetPath;
  protected Condition[] conditions = Condition.EMPTY_ARRAY;

  public AbstractTask(String targetPath) {
    this.targetPath = targetPath;
  }

  @Override
  public String getTargetPath() {
    return targetPath;
  }

  @Override
  public Condition[] getConditions() {
    return conditions;
  }

  @Nonnull
  public AbstractTask<T> withCondition(@Nonnull Condition condition) {
    this.conditions = ArrayUtils.add(conditions, condition);
    return this;
  }

  @Nonnull
  public AbstractTask<T> onlyIfFailed(@Nonnull Task<?> task) {
    return withCondition(new Task.StateCondition(task, TaskState.FAILED));
  }

  @Nonnull
  public AbstractTask<?> onlyIfAllFailed(@Nonnull Task<?>... tasks) {
    List<Task.Condition> conditions = new ArrayList<>();
    for (Task<?> task : tasks) conditions.add(new Task.StateCondition(task, TaskState.FAILED));
    return withCondition(new AndCondition(conditions));
  }

  /**
   * Runs the task.
   *
   * @param sink The ByteSink to which to write the data.
   * @param handle The Handle returned from {@link Connector#open}.
   * @throws Exception If the task fails.
   */
  @CheckForNull
  // @VisibleForTesting @ForOverride
  protected abstract T doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception;

  @Override
  public T run(TaskRunContext context) throws Exception {
    OutputHandle sink = context.newOutputFileHandle(getTargetPath());
    if (sink.exists()) {
      LOG.info("Skipping " + getName() + ": " + sink + " already exists.");
      return null;
    }
    T result = doRun(context, sink.asTemporaryByteSink(), context.getHandle());
    sink.commit();
    return result;
  }
}
