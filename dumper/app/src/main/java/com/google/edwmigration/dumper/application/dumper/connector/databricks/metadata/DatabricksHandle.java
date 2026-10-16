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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata;

import static com.google.common.collect.Iterables.skip;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.ByteSink;
import com.google.common.io.Closer;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper;
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask.TargetInitialization;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask.TaskOptions;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.TableConstraintsFormat;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.function.IOBiConsumer;
import org.apache.commons.io.function.IOFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe shared {@code Handle} and CSV metadata task implementations for the Databricks
 * metadata connector.
 */
@ThreadSafe
public final class DatabricksHandle extends AbstractHandle {

  private static final String CHECK_TASK_TARGET_PATH = "databricks-metadata-check.txt";

  private static final Logger logger = LoggerFactory.getLogger(DatabricksHandle.class);

  /** The prefix of the table properties in which Delta stores the CHECK constraints of a table. */
  private static final String CHECK_CONSTRAINT_PROPERTY_PREFIX = "delta.constraints.";
  // A record is an Object[] indexed by Header.ordinal(), which is the column order that
  // FORMAT.withHeader(Header.class) writes, so each value is printed under its column.
  private static final int TABLE_CONSTRAINTS_RECORD_SIZE =
      TableConstraintsFormat.Header.values().length;

  private final DatabricksSqlHelper sqlHelper;
  private final WorkspaceClient client;
  private final DatabricksFilter filter;

  /**
   * Creates the handle of a run. {@code sqlHelper} is null when no SQL warehouse was given; then
   * only the Unity Catalog tasks run, because they read the REST API only.
   */
  public DatabricksHandle(
      DatabricksSqlHelper sqlHelper, WorkspaceClient client, DatabricksFilter filter) {
    this.sqlHelper = sqlHelper;
    this.client = Preconditions.checkNotNull(client, "WorkspaceClient was null.");
    this.filter = Preconditions.checkNotNull(filter, "DatabricksFilter was null.");
  }

  /**
   * Returns a {@link TaskCategory#REQUIRED} task that fails the run when every task in {@code
   * tasks} failed, because the metadata tasks themselves are {@link TaskCategory#OPTIONAL}.
   */
  public static Task<?> createAllFailedCheckTask(List<Task<?>> tasks) {
    return new AbstractTask<Void>(
        CHECK_TASK_TARGET_PATH,
        TaskOptions.DEFAULT.withTargetInitialization(TargetInitialization.DO_NOT_CREATE)) {
      @Override
      protected String describeSourceData() {
        return "Check whether all Databricks metadata tasks failed";
      }

      @Override
      protected Void doRun(TaskRunContext context, ByteSink sink, Handle handle)
          throws IOException {
        throw new IOException(
            "All Databricks metadata tasks failed. Check the task exceptions above for details.");
      }
    }.onlyIfAllFailed(tasks.toArray(new Task<?>[0]));
  }

  /**
   * Prints a row of {@code table_constraints.csv} or {@code table_constraints-hms.csv} for each
   * CHECK constraint of a Delta table, in name order. Delta stores {@code ALTER TABLE ... ADD
   * CONSTRAINT valid_id CHECK (id > 0)} as the table property {@code delta.constraints.valid_id} =
   * {@code id > 0}, so the constraints are read from {@code properties}, the JSON object of the
   * table properties, which is missing for a table with only the BROWSE privilege.
   */
  static void printCheckConstraints(
      CSVPrinter printer, String catalog, String schema, String table, JsonNode properties)
      throws IOException {
    ImmutableSortedMap.Builder<String, String> clauses = ImmutableSortedMap.naturalOrder();
    Iterator<Map.Entry<String, JsonNode>> fields = properties.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      if (field.getKey().startsWith(CHECK_CONSTRAINT_PROPERTY_PREFIX)) {
        clauses.put(
            field.getKey().substring(CHECK_CONSTRAINT_PROPERTY_PREFIX.length()),
            field.getValue().asText(""));
      }
    }

    for (Map.Entry<String, String> clause : clauses.build().entrySet()) {
      Object[] record = new Object[TABLE_CONSTRAINTS_RECORD_SIZE];
      record[TableConstraintsFormat.Header.ConstraintCatalog.ordinal()] = catalog;
      record[TableConstraintsFormat.Header.ConstraintSchema.ordinal()] = schema;
      record[TableConstraintsFormat.Header.ConstraintName.ordinal()] = clause.getKey();
      record[TableConstraintsFormat.Header.TableCatalog.ordinal()] = catalog;
      record[TableConstraintsFormat.Header.TableSchema.ordinal()] = schema;
      record[TableConstraintsFormat.Header.TableName.ordinal()] = table;
      record[TableConstraintsFormat.Header.ConstraintType.ordinal()] = "CHECK";
      record[TableConstraintsFormat.Header.CheckClause.ordinal()] = clause.getValue();
      printer.printRecord(record);
    }
  }

  /**
   * Returns the SQL helper of the hive_metastore tasks, or null when no SQL warehouse was given.
   */
  public DatabricksSqlHelper sqlHelper() {
    return this.sqlHelper;
  }

  /** Returns the client of the Unity Catalog tasks, which read the REST API. */
  WorkspaceClient client() {
    return this.client;
  }

  public DatabricksFilter filter() {
    return this.filter;
  }

  /**
   * Executes {@code call} sequentially for each element of {@code inputs} and forwards each
   * successful result to {@code onSuccess} in input order. Logs and skips per-object {@link
   * IOException} and {@link DatabricksException} failures, and throws an {@link IOException} if
   * {@code call} fails for every object in a non-empty {@code inputs}, or an {@link
   * InterruptedIOException} if the thread is interrupted. The objects are logged with their {@code
   * toString()}, and a {@link RecordProgressMonitor} logs the progress at DEBUG level.
   *
   * @return the warning for the task summary when only some objects fail, such as {@code tables.get
   *     failed for 1 of 3 objects}, which is also logged, or null when no object fails
   */
  @Nullable
  <I, O> String runForEach(
      String callName, Collection<I> inputs, IOFunction<I, O> call, IOBiConsumer<I, O> onSuccess)
      throws IOException {
    return runForEach(callName, inputs, call, onSuccess, IOBiConsumer.noop());
  }

  /**
   * Like {@link #runForEach(String, Collection, IOFunction, IOBiConsumer)}, and also passes each
   * skipped object and its failure to {@code onFailure}, in input order with the results.
   *
   * @return the warning for the task summary when only some objects fail, or null when no object
   *     fails
   */
  @Nullable
  <I, O> String runForEach(
      String callName,
      Collection<I> inputs,
      IOFunction<I, O> call,
      IOBiConsumer<I, O> onSuccess,
      IOBiConsumer<I, Exception> onFailure)
      throws IOException {
    int successCount = 0;
    int failureCount = 0;
    Exception firstFailure = null;
    try (RecordProgressMonitor monitor = new RecordProgressMonitor(callName, inputs.size())) {
      for (I input : inputs) {
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedIOException("Interrupted while running " + callName + ".");
        }

        O result;
        try {
          result = call.apply(input);
        } catch (IOException | DatabricksException e) {
          failureCount++;
          if (firstFailure == null) {
            firstFailure = e;
          }
          logger.warn("Skipping {} for {}: {}", callName, input, e.getMessage());
          onFailure.accept(input, e);
          continue;
        } finally {
          monitor.count();
        }

        // Outside the try, so that a failure to write the output, such as an IOException of the
        // CSV printer, fails the task instead of skipping the object.
        onSuccess.accept(input, result);
        successCount++;
      }
    }

    if (failureCount == 0) {
      return null;
    }

    if (successCount == 0) {
      throw new IOException(
          String.format(
              "%s failed for all %d objects. First failure: %s",
              callName, failureCount, firstFailure.getMessage()),
          firstFailure);
    }

    String warning =
        String.format(
            "%s failed for %d of %d objects", callName, failureCount, failureCount + successCount);
    logger.warn("{}.", warning);
    return warning;
  }

  /** Returns the warnings for the summary of a metadata task, leaving out the nulls. */
  static ImmutableList<String> constructWarnings(String... warnings) {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (String warning : warnings) {
      if (warning != null) {
        builder.add(warning);
      }
    }

    return builder.build();
  }

  /**
   * Returns the description of a task followed by its warnings, if any, such as {@code from Unity
   * Catalog tables.get (WARNING: tables.get failed for 1 of 3 objects)}.
   */
  private static String describeWithWarnings(String description, List<String> warnings) {
    return warnings.isEmpty()
        ? description
        : description + " (WARNING: " + String.join("; ", warnings) + ")";
  }

  /** A metadata task that writes rows for a header enum to a CSV zip entry. */
  public static final class MetadataTask extends AbstractTask<Void> {

    /**
     * Writes the rows of one metadata CSV file, in the column order of its header, and returns the
     * warnings for the task summary.
     */
    @FunctionalInterface
    public interface RowSource {
      ImmutableList<String> writeRows(DatabricksHandle handle, CSVPrinter printer)
          throws IOException;
    }

    private final Class<? extends Enum<?>> header;
    private final String sourceDescription;
    private final RowSource rowSource;
    private ImmutableList<String> warnings = ImmutableList.of();

    /**
     * {@code sourceDescription} completes the task description shown in the dry run and the task
     * summary, such as {@code from Unity Catalog catalogs.list}.
     */
    MetadataTask(
        String targetPath,
        Class<? extends Enum<?>> header,
        String sourceDescription,
        RowSource rowSource) {
      super(targetPath);
      this.header = header;
      this.sourceDescription = sourceDescription;
      this.rowSource = rowSource;
    }

    @Override
    public TaskCategory getCategory() {
      return TaskCategory.OPTIONAL;
    }

    @Override
    protected String describeSourceData() {
      return describeWithWarnings(this.sourceDescription, this.warnings);
    }

    @Override
    protected Void doRun(TaskRunContext context, ByteSink sink, Handle handle) throws Exception {
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter printer = FORMAT.withHeader(this.header).print(writer)) {
        this.warnings = this.rowSource.writeRows((DatabricksHandle) handle, printer);
      }
      return null;
    }
  }

  /**
   * A metadata task that writes several CSV zip entries in one pass over its source, so that each
   * object is read once for all of them. The first file is the target of the task; the other files
   * are committed only when the task succeeds.
   */
  public static final class MultiFileMetadataTask extends AbstractTask<Void> {

    /**
     * Writes the rows of the files of a task, given their CSV printers by zip entry name, and
     * returns the warnings for the task summary.
     */
    @FunctionalInterface
    public interface RowSource {
      ImmutableList<String> writeRows(DatabricksHandle handle, Map<String, CSVPrinter> printers)
          throws IOException;
    }

    private final ImmutableMap<String, Class<? extends Enum<?>>> files;
    private final String sourceDescription;
    private final RowSource rowSource;
    private ImmutableList<String> warnings = ImmutableList.of();

    /**
     * {@code files} maps the zip entry names of the files to their headers, the target first. The
     * task description lists the other files after {@code sourceDescription}.
     */
    MultiFileMetadataTask(
        ImmutableMap<String, Class<? extends Enum<?>>> files,
        String sourceDescription,
        RowSource rowSource) {
      super(files.keySet().iterator().next());
      this.files = files;
      this.sourceDescription = sourceDescription;
      this.rowSource = rowSource;
    }

    @Override
    public TaskCategory getCategory() {
      return TaskCategory.OPTIONAL;
    }

    /**
     * Returns the source description, the other files, such as {@code (with columns.csv)}, and the
     * warnings, if any.
     */
    @Override
    protected String describeSourceData() {
      return describeWithWarnings(
          this.sourceDescription
              + " (with "
              + String.join(", ", skip(this.files.keySet(), 1))
              + ")",
          this.warnings);
    }

    @Override
    protected Void doRun(TaskRunContext context, ByteSink sink, Handle handle) throws Exception {
      List<OutputHandle> otherFiles = new ArrayList<>();
      try (Closer closer = Closer.create()) {
        ImmutableMap.Builder<String, CSVPrinter> printers = ImmutableMap.builder();
        for (Map.Entry<String, Class<? extends Enum<?>>> file : this.files.entrySet()) {
          ByteSink fileSink = sink;
          if (!file.getKey().equals(getTargetPath())) {
            OutputHandle otherFile = context.newOutputFileHandle(file.getKey());
            otherFiles.add(otherFile);
            fileSink = otherFile.asTemporaryByteSink();
          }
          Writer writer = closer.register(fileSink.asCharSink(UTF_8).openBufferedStream());
          printers.put(
              file.getKey(), closer.register(FORMAT.withHeader(file.getValue()).print(writer)));
        }
        this.warnings = this.rowSource.writeRows((DatabricksHandle) handle, printers.build());
      }
      // Commit after the Closer block closes the writers, because OutputHandle.close() commits.
      for (OutputHandle otherFile : otherFiles) {
        otherFile.commit();
      }
      return null;
    }
  }

  /** The catalogs and schemas selected by {@code --database} and {@code --schema}. */
  public static final class DatabricksFilter {

    public static final String HIVE_METASTORE_CATALOG = "hive_metastore";
    private static final String INFORMATION_SCHEMA = "information_schema";

    private static final ImmutableSet<String> UNLISTED_EXCLUDED_CATALOGS =
        ImmutableSet.of("system", "samples", "__databricks_internal");

    private final ImmutableSet<String> includedCatalogs;
    private final ImmutableSet<String> includedSchemas;

    /** The names are trimmed and not empty, as {@link ConnectorArguments} returns them. */
    @VisibleForTesting
    DatabricksFilter(Collection<String> rawCatalogs, Collection<String> rawSchemas) {
      this.includedCatalogs = normalizeSet(rawCatalogs);
      this.includedSchemas = normalizeSet(rawSchemas);
    }

    public static DatabricksFilter fromArguments(ConnectorArguments arguments) {
      return new DatabricksFilter(arguments.getDatabases(), arguments.getSchemata());
    }

    private static ImmutableSet<String> normalizeSet(Collection<String> values) {
      ImmutableSet.Builder<String> builder = ImmutableSet.builder();
      for (String value : values) {
        builder.add(normalize(value));
      }
      return builder.build();
    }

    private static String normalize(String name) {
      return name.toLowerCase(Locale.ROOT);
    }

    public boolean acceptsCatalog(String catalogName) {
      String normalized = normalize(catalogName);
      if (includedCatalogs.isEmpty()) {
        return !UNLISTED_EXCLUDED_CATALOGS.contains(normalized);
      }
      return includedCatalogs.contains(normalized);
    }

    /** Returns whether the filter accepts a catalog other than {@code hive_metastore}. */
    public boolean acceptsUnityCatalog() {
      return !includedCatalogs.equals(ImmutableSet.of(HIVE_METASTORE_CATALOG));
    }

    boolean acceptsSchema(String schemaName) {
      String normalized = normalize(schemaName);
      return !INFORMATION_SCHEMA.equals(normalized)
          && (includedSchemas.isEmpty() || includedSchemas.contains(normalized));
    }
  }
}
