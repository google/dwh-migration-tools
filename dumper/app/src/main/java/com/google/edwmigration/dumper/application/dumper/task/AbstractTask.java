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

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle.WriteMode;
import com.google.errorprone.annotations.ForOverride;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
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
  protected final TaskOptions options;
  protected Condition[] conditions = Condition.EMPTY_ARRAY;

  public AbstractTask(@Nonnull String targetPath) {
    this(targetPath, TaskOptions.DEFAULT);
  }

  public AbstractTask(@Nonnull String targetPath, @Nonnull TaskOptions options) {
    Preconditions.checkNotNull(options, "Task options was null.");
    this.targetPath = targetPath;
    this.options = options;
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
  public final T run(@Nonnull TaskRunContext context) throws Exception {
    return getWrapper(context).runTask(this, context).orElse(null);
  }

  SinkWrapper getWrapper(@Nonnull TaskRunContext context) throws IOException {
    if (options.targetInitialization() == TargetInitialization.DO_NOT_CREATE) {
      return SinkWrapper.decoy();
    }
    OutputHandle handle = context.newOutputFileHandle(getTargetPath());
    if (options.writeMode().equals(OutputHandle.WriteMode.APPEND_EXISTING)) {
      return SinkWrapper.append(handle, options);
    } else if (handle.exists()) {
      return SinkWrapper.skip(handle);
    } else {
      return SinkWrapper.temporary(handle, options);
    }
  }

  static class SinkWrapper {
    @Nullable final ByteSink sink;
    @Nullable final OutputHandle handle;
    final boolean shouldCommit;

    @Nonnull
    static SinkWrapper decoy() {
      return new SinkWrapper(DummyByteSink.INSTANCE, null, false);
    }

    @Nonnull
    static SinkWrapper append(@Nonnull OutputHandle handle, @Nonnull TaskOptions options)
        throws IOException {
      return new SinkWrapper(handle.asByteSink(options.writeMode()), handle, false);
    }

    @Nonnull
    static SinkWrapper skip(@Nonnull OutputHandle handle) {
      return new SinkWrapper(null, handle, false);
    }

    @Nonnull
    static SinkWrapper temporary(@Nonnull OutputHandle handle, @Nonnull TaskOptions options)
        throws IOException {
      return new SinkWrapper(handle.asTemporaryByteSink(options.writeMode()), handle, true);
    }

    SinkWrapper(@Nullable ByteSink sink, @Nullable OutputHandle handle, boolean shouldCommit) {
      this.sink = sink;
      this.handle = handle;
      this.shouldCommit = shouldCommit;
    }

    @Nonnull
    <U> Optional<U> runTask(AbstractTask<U> task, TaskRunContext context) throws Exception {
      ByteSink localSink = sink;
      if (localSink != null) {
        U result = task.doRun(context, localSink, context.getHandle());
        if (handle != null && shouldCommit) {
          handle.commit();
        }
        return Optional.ofNullable(result);
      } else {
        logger.info("Skipping {}. Reason: {} already exists.", task.getName(), handle);
        return Optional.empty();
      }
    }
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
    return options.targetInitialization() == TargetInitialization.CREATE
        ? format(getToStringTemplate(), targetPath, describeSourceData())
        : describeSourceData();
  }

  private String getToStringTemplate() {
    WriteMode writeMode = options.writeMode();
    switch (writeMode) {
      case CREATE_TRUNCATE:
        return "Write %s %s";
      case APPEND_EXISTING:
        return "Append %s %s";
      default:
        throw new UnsupportedOperationException("Unsupported write mode: " + writeMode);
    }
  }

  public enum TargetInitialization {
    CREATE,
    DO_NOT_CREATE
  }

  static class DummyByteSink extends ByteSink {

    static final DummyByteSink INSTANCE = new DummyByteSink();

    @Override
    public OutputStream openStream() {
      throw new UnsupportedOperationException("Opening stream for DummyByteSink is not supported.");
    }
  }

  @AutoValue
  @ParametersAreNonnullByDefault
  public abstract static class TaskOptions {
    public static final TaskOptions DEFAULT = builder().build();

    abstract TargetInitialization targetInitialization();

    abstract OutputHandle.WriteMode writeMode();

    abstract Builder toBuilder();

    public final TaskOptions withWriteMode(WriteMode writeMode) {
      return toBuilder().setWriteMode(writeMode).build();
    }

    public final TaskOptions withTargetInitialization(TargetInitialization targetInitialization) {
      return toBuilder().setTargetInitialization(targetInitialization).build();
    }

    public static Builder builder() {
      return new AutoValue_AbstractTask_TaskOptions.Builder()
          .setTargetInitialization(TargetInitialization.CREATE)
          .setWriteMode(WriteMode.CREATE_TRUNCATE);
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTargetInitialization(TargetInitialization value);

      public abstract Builder setWriteMode(WriteMode value);

      public abstract TaskOptions build();
    }
  }
}
