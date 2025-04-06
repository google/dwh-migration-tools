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

import static com.google.edwmigration.dumper.application.dumper.SummaryPrinter.joinSummaryDoubleLine;
import static java.lang.String.format;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import com.google.errorprone.annotations.ForOverride;
import java.beans.PropertyDescriptor;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

/** @author shevek */
public abstract class AbstractTask<T> implements Task<T> {

  private static final Logger logger = LoggerFactory.getLogger(AbstractTask.class);

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
  private final TargetInitialization targetInitialization;
  protected Condition[] conditions = Condition.EMPTY_ARRAY;

  public AbstractTask(String targetPath) {
    this(targetPath, TargetInitialization.CREATE);
  }

  public AbstractTask(String targetPath, TargetInitialization targetInitialization) {
    Preconditions.checkNotNull(
        targetInitialization, "Target initialization behavior must be defined.");
    this.targetPath = targetPath;
    this.targetInitialization = targetInitialization;
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
  @ForOverride
  protected abstract T doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception;

  @Override
  public T run(TaskRunContext context) throws Exception {
    if (targetInitialization == TargetInitialization.DO_NOT_CREATE) {
      return doRun(context, DummyByteSink.INSTANCE, context.getHandle());
    }

    OutputHandle sink = context.newOutputFileHandle(getTargetPath());
    if (sink.exists()) {
      logger.info("Skipping {}: {} already exists.", getName(), sink);
      return null;
    }
    T result = doRun(context, sink.asTemporaryByteSink(), context.getHandle());
    sink.commit();
    return result;
  }

  protected static CSVFormat newCsvFormatForClass(Class<?> clazz) {
    CSVFormat format =
        FORMAT.withHeader(
            Arrays.stream(BeanUtils.getPropertyDescriptors(clazz))
                .map(PropertyDescriptor::getName)
                .toArray(String[]::new));
    return format;
  }

  protected String describeSourceData() {
    return "from " + getClass().getSimpleName();
  }

  /**
   * Returns the source data description for the SQL query.
   *
   * @param query SQL query
   * @return the source data description
   */
  protected static String createSourceDataDescriptionForQuery(String query) {
    return joinSummaryDoubleLine("from", query);
  }

  @Override
  public String toString() {
    return targetInitialization == TargetInitialization.CREATE
        ? format("Write %s %s", targetPath, describeSourceData())
        : describeSourceData();
  }

  public enum TargetInitialization {
    CREATE,
    DO_NOT_CREATE
  }

  private static class DummyByteSink extends ByteSink {
    private static final DummyByteSink INSTANCE = new DummyByteSink();

    @Override
    public OutputStream openStream() {
      throw new UnsupportedOperationException("Opening stream for DummyByteSink is not supported.");
    }
  }
}
