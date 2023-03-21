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
package com.google.edwmigration.dumper.application.dumper.connector.bigquery;

import com.google.api.gax.paging.Page;
import com.google.auto.service.AutoService;
import com.google.cloud.StringEnumValue;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.MaterializedViewDefinition;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.plugin.ext.bigquery.BigQueryCallable;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.ext.jdk.concurrent.ExecutorManager;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.ProgressMonitor;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.BigQueryMetadataDumpFormat;
import com.swrve.ratelimitedlogger.RateLimitedLog;
import java.io.IOException;
import java.io.Writer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.BooleanUtils;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from Google BigQuery.")
@RespectsInput(
    order = 600,
    arg = ConnectorArguments.OPT_DATABASE,
    description = "The list of projects from which to dump, separated by commas.")
@RespectsInput(
    order = 2000,
    arg = ConnectorArguments.OPT_SCHEMA,
    description = "The list of datasets to dump, separated by commas.")
public class BigQueryMetadataConnector extends AbstractBigQueryConnector
    implements BigQueryMetadataDumpFormat, MetadataConnector {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMetadataConnector.class);

  private static final Logger LOG_LIMITED =
      RateLimitedLog.withRateLimit(LOG).maxRate(2).every(Duration.ofSeconds(10)).build();

  @PolyNull
  private static String toEnumName(@PolyNull Enum<?> value) {
    return value == null ? null : value.name();
  }

  @PolyNull
  private static String toEnumName(@PolyNull StringEnumValue value) {
    return value == null ? null : value.name();
  }

  public abstract static class AbstractBigQueryMetadataTask extends AbstractBigQueryTask {

    private final List<? extends String> databaseList;
    private final Predicate<? super String> schemaPredicate;

    public AbstractBigQueryMetadataTask(
        String targetPath,
        @Nonnull List<? extends String> databaseList,
        Predicate<? super String> schemaPredicate) {
      super(targetPath);
      this.databaseList = Preconditions.checkNotNull(databaseList, "Database list was null.");
      this.schemaPredicate =
          Preconditions.checkNotNull(schemaPredicate, "Schema predicate was null.");
    }

    protected boolean isIncludedDataset(@Nonnull Dataset dataset) {
      boolean out =
          schemaPredicate.test(dataset.getDatasetId().getDataset())
              || schemaPredicate.test(
                  dataset.getFriendlyName()); // friendlyName is typically null because it's from a
      // list() not a reload().
      if (LOG.isTraceEnabled())
        LOG.trace(
            dataset.getDatasetId().getDataset()
                + "("
                + dataset.getFriendlyName()
                + ") : "
                + out
                + " (via "
                + schemaPredicate
                + ")");
      return out;
    }

    protected static interface BigQueryConsumer<V> {

      public void accept(@Nonnull V value)
          throws BigQueryException, IOException, InterruptedException;
    }

    private void forEachDataset(
        @Nonnull BigQuery bigQuery,
        @Nonnull BigQueryConsumer<? super Dataset> consumer,
        @Nonnull BigQueryCallable<Page<Dataset>> callable)
        throws BigQueryException, IOException, InterruptedException {
      Page<Dataset> datasets = runWithBackOff(callable);
      // LOG.debug("List is " + datasets.getValues());
      for (Dataset dataset : new PageIterable<>(datasets)) {
        if (!isIncludedDataset(dataset)) continue;
        consumer.accept(dataset);
      }
    }

    protected void forEachDataset(
        @Nonnull BigQuery bigQuery, @Nonnull BigQueryConsumer<? super Dataset> consumer)
        throws BigQueryException, IOException, InterruptedException {
      if (databaseList.isEmpty()) {
        // Use the default project.
        forEachDataset(
            bigQuery,
            consumer,
            () -> bigQuery.listDatasets(BigQuery.DatasetListOption.pageSize(100)));
      } else {
        for (String projectName : databaseList) {
          // LOG.debug("Listing datasets in  " + projectName);
          forEachDataset(
              bigQuery,
              consumer,
              () -> bigQuery.listDatasets(projectName, BigQuery.DatasetListOption.pageSize(100)));
        }
      }
    }
  }

  public static class DatasetsTask extends AbstractBigQueryMetadataTask
      implements BigQueryMetadataDumpFormat.DatasetsTaskFormat {

    public DatasetsTask(
        @Nonnull List<? extends String> databaseList,
        @Nonnull Predicate<? super String> schemaPredicate) {
      super(ZIP_ENTRY_NAME, databaseList, schemaPredicate);
    }

    @Override
    public TaskCategory getCategory() {
      return TaskCategory.OPTIONAL;
    }

    private void add(@Nonnull CSVPrinter printer, @Nonnull Dataset _dataset)
        throws IOException, InterruptedException {
      Dataset dataset =
          runWithBackOff(
              () ->
                  _dataset.reload(
                      BigQuery.DatasetOption.fields(
                          BigQuery.DatasetField.FRIENDLY_NAME, BigQuery.DatasetField.LOCATION)));
      if (dataset == null) {
        // contractually non-null, but null seen during testing
        LOG_LIMITED.warn(
            "BigQuery returned a null Dataset object from reload({}), which we will ignore.",
            _dataset.getDatasetId());
        return;
      }
      DatasetId datasetId = dataset.getDatasetId();
      synchronized (printer) {
        printer.printRecord(
            datasetId.getProject(),
            datasetId.getDataset(),
            dataset.getFriendlyName(),
            dataset.getLocation());
      }
    }

    @Override
    protected void run(Writer writer, BigQuery bigQuery) throws Exception {
      ExecutorService executor = newExecutorService();
      try (CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
          ExecutorManager manager = new ExecutorManager(executor);
          RecordProgressMonitor monitor =
              new RecordProgressMonitor("Writing to " + getTargetPath())) {
        forEachDataset(
            bigQuery,
            dataset -> {
              monitor.count();
              if (!isIncludedDataset(dataset)) return;
              manager.execute(
                  () -> {
                    add(printer, dataset);
                    return null;
                  });
            });
      } finally {
        shutdown(executor);
      }
    }

    @Override
    protected String toCallDescription() {
      return "BigQuery.listDatasets()";
    }
  }

  public static class TablesJsonTask extends AbstractBigQueryMetadataTask
      implements BigQueryMetadataDumpFormat.TablesJsonTaskFormat {

    public TablesJsonTask(
        @Nonnull List<? extends String> databaseList,
        @Nonnull Predicate<? super String> schemaPredicate) {
      super(ZIP_ENTRY_NAME, databaseList, schemaPredicate);
    }

    @Nonnull
    private static Metadata.Field toField(@Nonnull Field field) {
      Metadata.Field out = new Metadata.Field();
      out.name = field.getName();
      out.type = toEnumName(field.getType());
      out.subFields = toFieldList(field.getSubFields());
      out.mode = toEnumName(field.getMode());
      out.description = field.getDescription();
      return out;
    }

    @CheckForNull
    private static List<Metadata.Field> toFieldList(@CheckForNull List<? extends Field> fields) {
      if (fields == null) return null;
      List<Metadata.Field> out = new ArrayList<>(fields.size());
      for (Field field : fields) out.add(toField(field));
      return out;
    }

    private static void addTable(@Nonnull Metadata metadata, @Nonnull Table table) {
      // printer.printRecord(tableId.getProject(), tableId.getDataset(), tableId.getTable(),
      // table.getFriendlyName(), tableDefinition.getType(), table.getNumRows(),
      // table.getNumBytes(), metadataText);
      TableId tableId = table.getTableId();
      metadata.project = tableId.getProject();
      metadata.dataset =
          tableId.getDataset(); // Apparently there is a precedent for this being null.
      metadata.table = tableId.getTable();
      metadata.friendlyName = table.getFriendlyName();
      metadata.creationTime = table.getCreationTime();
      metadata.expirationTime = table.getExpirationTime();

      TableDefinition tableDefinition = table.getDefinition();
      if (tableDefinition != null) {
        metadata.tableType = tableDefinition.getType().name();

        Schema tableSchema = tableDefinition.getSchema();
        if (tableSchema != null) {
          metadata.schema = toFieldList(tableSchema.getFields());
        }
      }
    }

    private static void addStandardTable(
        @Nonnull Metadata metadata, @Nonnull StandardTableDefinition standardTableDefinition) {
      TimePartitioning timePartitioning =
          standardTableDefinition
              .getTimePartitioning(); // field, type (period), partitioning_required
      if (timePartitioning != null) {
        metadata.timePartitioningField = timePartitioning.getField();
        metadata.timePartitioningRequired =
            BooleanUtils.isTrue(timePartitioning.getRequirePartitionFilter());
      }
      RangePartitioning rangePartitioning =
          standardTableDefinition.getRangePartitioning(); // field, range
      // TODO, when it matters.
    }

    private static void addExternalTable(
        @Nonnull Metadata metadata, @Nonnull ExternalTableDefinition in) {
      // in.getSourceUris();
      // TODO:
    }

    private static void addView(
        @Nonnull Metadata metadata, @Nonnull ViewDefinition viewDefinition) {
      metadata.viewQuery = viewDefinition.getQuery();
    }

    private static void addMaterializedView(
        @Nonnull Metadata metadata,
        @Nonnull MaterializedViewDefinition materializedViewDefinition) {
      metadata.viewQuery = materializedViewDefinition.getQuery();
    }

    private static void add(@Nonnull Writer writer, @Nonnull Table _table)
        throws IOException, InterruptedException {
      Table table =
          runWithBackOff(
              () ->
                  _table.reload(
                      BigQuery.TableOption.fields(
                          BigQuery.TableField.FRIENDLY_NAME,
                          BigQuery.TableField.DESCRIPTION,
                          BigQuery.TableField.TYPE,
                          BigQuery.TableField.VIEW,
                          BigQuery.TableField.NUM_ROWS,
                          BigQuery.TableField.NUM_BYTES,
                          BigQuery.TableField.SCHEMA,
                          BigQuery.TableField.EXTERNAL_DATA_CONFIGURATION,
                          BigQuery.TableField.TIME_PARTITIONING,
                          BigQuery.TableField.EXPIRATION_TIME,
                          BigQuery.TableField.CREATION_TIME)));
      if (table == null) {
        // contractually non-null, but null seen during testing
        LOG_LIMITED.warn(
            "BigQuery returned a null Table object from reload({}), which we will ignore (perhaps the dataset in use is empty?)",
            _table.getTableId());
        return;
      }
      TableId tableId = table.getTableId();
      TableDefinition tableDefinition = table.getDefinition();

      Metadata metadata = new Metadata();
      addTable(metadata, table);
      if (tableDefinition instanceof StandardTableDefinition) {
        addStandardTable(metadata, (StandardTableDefinition) tableDefinition);
      } else if (tableDefinition instanceof ExternalTableDefinition) {
        addExternalTable(metadata, (ExternalTableDefinition) tableDefinition);
      } else if (tableDefinition instanceof ViewDefinition) {
        addView(metadata, (ViewDefinition) tableDefinition);
      } else if (tableDefinition instanceof MaterializedViewDefinition) {
        addMaterializedView(metadata, (MaterializedViewDefinition) tableDefinition);
      } else {
        LOG.debug("Unknown table definition type: " + tableDefinition.getClass());
      }

      String metadataText = BigQueryMetadataDumpFormat.MAPPER.writeValueAsString(metadata);
      synchronized (writer) {
        writer.write(metadataText);
        writer.write('\n');
      }
    }

    @Override
    protected void run(Writer writer, BigQuery bigQuery) throws Exception {
      ExecutorService executor = newExecutorService();
      try (ProgressMonitor monitor = new RecordProgressMonitor("Writing to " + getTargetPath());
          ExecutorManager manager = new ExecutorManager(executor)) {
        forEachDataset(
            bigQuery,
            dataset -> {
              if (!isIncludedDataset(dataset)) return;
              Page<Table> tables =
                  runWithBackOff(
                      () ->
                          bigQuery.listTables(
                              dataset.getDatasetId(), BigQuery.TableListOption.pageSize(100)));
              for (Table table : new PageIterable<>(tables)) {
                monitor.count();
                manager.execute(
                    () -> {
                      add(writer, table);
                      return null;
                    });
              }
            });
      } finally {
        shutdown(executor);
      }
    }

    @Override
    protected String toCallDescription() {
      return "BigQuery.listTables()";
    }
  }

  public BigQueryMetadataConnector() {
    super("bigquery");
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    List<? extends String> databaseList = arguments.getDatabases();
    Predicate<? super String> schemaPredicate = arguments.getSchemaPredicate();
    out.add(new DatasetsTask(databaseList, schemaPredicate));
    out.add(new TablesJsonTask(databaseList, schemaPredicate));
    // out.add(new ColumnsTask(databaseList, schemaPredicate));
  }
}
