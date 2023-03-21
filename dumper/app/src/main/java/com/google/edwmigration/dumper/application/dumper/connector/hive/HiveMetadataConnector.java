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
package com.google.edwmigration.dumper.application.dumper.connector.hive;

import com.google.auto.service.AutoService;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentSchemaPredicate;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.ext.hive.metastore.Database;
import com.google.edwmigration.dumper.ext.hive.metastore.Field;
import com.google.edwmigration.dumper.ext.hive.metastore.Function;
import com.google.edwmigration.dumper.ext.hive.metastore.HiveMetastoreThriftClient;
import com.google.edwmigration.dumper.ext.hive.metastore.Partition;
import com.google.edwmigration.dumper.ext.hive.metastore.PartitionKey;
import com.google.edwmigration.dumper.ext.hive.metastore.Table;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.ConcurrentProgressMonitor;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.ConcurrentRecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.HiveMetadataDumpFormat;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RespectsArgumentSchemaPredicate
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from the Hive metastore via Thrift.")
public class HiveMetadataConnector extends AbstractHiveConnector
    implements HiveMetadataDumpFormat, MetadataConnector {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetadataConnector.class);

  private abstract static class AbstractHiveMetadataTask extends AbstractHiveTask {

    private final Predicate<String> schemaPredicate;

    private AbstractHiveMetadataTask(
        String targetPath, @Nonnull Predicate<String> schemaPredicate) {
      super(targetPath);
      this.schemaPredicate =
          Preconditions.checkNotNull(schemaPredicate, "Schema predicate was null.");
    }

    protected boolean isIncludedSchema(@Nonnull String schema) {
      return schemaPredicate.test(schema);
    }
  }

  private static class SchemataTask extends AbstractHiveMetadataTask implements SchemataFormat {

    private SchemataTask(@Nonnull Predicate<String> schemaPredicate) {
      super(ZIP_ENTRY_NAME, schemaPredicate);
    }

    @Override
    protected void run(@Nonnull Writer writer, @Nonnull ThriftClientHandle thriftClientHandle)
        throws Exception {
      try (HiveMetastoreThriftClient client = thriftClientHandle.newClient("schemata-task-client");
          RecordProgressMonitor monitor =
              new RecordProgressMonitor("Writing database names to " + getTargetPath());
          CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer)) {
        List<? extends String> allDatabases = client.getAllDatabaseNames();
        for (String database : allDatabases) {
          monitor.count();
          printer.printRecord(database);
        }
      }
    }

    @Override
    protected String toCallDescription() {
      return "get_all_databases()";
    }
  }

  private static class DatabasesTask extends AbstractHiveMetadataTask implements DatabasesFormat {

    private DatabasesTask(@Nonnull Predicate<String> schemaPredicate) {
      super(ZIP_ENTRY_NAME, schemaPredicate);
    }

    @Override
    protected void run(@Nonnull Writer writer, @Nonnull ThriftClientHandle thriftClientHandle)
        throws Exception {
      try (HiveMetastoreThriftClient client =
              thriftClientHandle.newClient("databases-task-client");
          RecordProgressMonitor monitor =
              new RecordProgressMonitor("Writing databases info to " + getTargetPath());
          CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer)) {
        List<? extends String> allDatabases = client.getAllDatabaseNames();
        for (String databaseName : allDatabases) {
          Database database = client.getDatabase(databaseName);
          monitor.count();
          printer.printRecord(
              database.getName(),
              database.getDescription(),
              database.getOwner(),
              database.getOwnerType(),
              database.getLocation());
        }
      }
    }

    @Override
    protected String toCallDescription() {
      return "get_all_databases()*.get_database()";
    }
  }

  private static class TablesJsonTask extends AbstractHiveMetadataTask
      implements TablesJsonTaskFormat {

    private final boolean isHiveMetastorePartitionMetadataDumpingEnabled;

    private TablesJsonTask(
        @Nonnull Predicate<String> schemaPredicate,
        boolean isHiveMetastorePartitionMetadataDumpingEnabled) {
      super(ZIP_ENTRY_NAME, schemaPredicate);
      this.isHiveMetastorePartitionMetadataDumpingEnabled =
          isHiveMetastorePartitionMetadataDumpingEnabled;
    }

    @Override
    protected void run(@Nonnull Writer writer, @Nonnull ThriftClientHandle thriftClientHandle)
        throws Exception {
      try (ThriftClientPool clientPool =
          thriftClientHandle.newMultiThreadedThriftClientPool("tables-task-pooled-client")) {
        clientPool.execute(
            thriftClient -> {
              List<? extends String> allDatabases = thriftClient.getAllDatabaseNames();
              for (String databaseName : allDatabases) {
                if (isIncludedSchema(databaseName)) {
                  List<? extends String> allTables =
                      thriftClient.getAllTableNamesInDatabase(databaseName);
                  try (ConcurrentProgressMonitor monitor =
                      new ConcurrentRecordProgressMonitor(
                          "Writing tables in schema '" + databaseName + "' to " + getTargetPath(),
                          allTables.size())) {
                    for (String tableName : allTables) {
                      dumpTable(monitor, writer, clientPool, databaseName, tableName);
                    }
                  }
                }
              }
            });
      }
    }

    private void dumpTable(
        @Nonnull ConcurrentProgressMonitor monitor,
        @Nonnull Writer writer,
        @Nonnull ThriftClientPool clientPool,
        @Nonnull String databaseName,
        @Nonnull String tableName) {
      clientPool.execute(
          (thriftClient) -> {
            try {
              monitor.count();
              Table table = thriftClient.getTable(databaseName, tableName);
              TableMetadata outTable = new TableMetadata();
              outTable.schemaName = table.getDatabaseName();
              outTable.name = table.getTableName();
              outTable.type = table.getTableType();
              outTable.createTime = table.getCreateTime();
              outTable.lastAccessTime = table.getLastAccessTime();
              outTable.owner = table.getOwner();
              outTable.viewText = table.getOriginalViewText();
              outTable.location = table.getLocation();
              outTable.lastDdlTime = table.getLastDdlTime();
              outTable.totalSize = table.getTotalSize();
              outTable.rawSize = table.getRawSize();
              outTable.rowsCount = table.getRowsCount();
              outTable.filesCount = table.getFilesCount();
              outTable.retention = table.getRetention();
              outTable.bucketsCount = table.getBucketsCount();
              outTable.isCompressed = table.isCompressed();

              outTable.fields = new ArrayList<>();
              for (Field field : table.getFields()) {
                TableMetadata.FieldMetadata fieldMetadata = new TableMetadata.FieldMetadata();
                fieldMetadata.name = field.getFieldName();
                fieldMetadata.type = field.getType();
                fieldMetadata.comment = field.getComment();
                outTable.fields.add(fieldMetadata);
              }
              outTable.partitionKeys = new ArrayList<>();
              for (PartitionKey partitionKey : table.getPartitionKeys()) {
                TableMetadata.PartitionKeyMetadata partitionKeyMetadata =
                    new TableMetadata.PartitionKeyMetadata();
                partitionKeyMetadata.name = partitionKey.getPartitionKeyName();
                partitionKeyMetadata.type = partitionKey.getType();
                partitionKeyMetadata.comment = partitionKey.getComment();
                outTable.partitionKeys.add(partitionKeyMetadata);
              }
              if (isHiveMetastorePartitionMetadataDumpingEnabled) {
                outTable.partitions = new ArrayList<>();
                // This call to the Thrift client is particularly expensive when many partitions are
                // present.
                for (Partition partition : table.getPartitions()) {
                  TableMetadata.PartitionMetadata partitionMetadata =
                      new TableMetadata.PartitionMetadata();
                  partitionMetadata.name = partition.getPartitionName();
                  partitionMetadata.location = partition.getLocation();
                  partitionMetadata.createTime = partition.getCreateTime();
                  partitionMetadata.lastAccessTime = partition.getLastAccessTime();
                  partitionMetadata.lastDdlTime = partition.getLastDdlTime();
                  partitionMetadata.totalSize = partition.getTotalSize();
                  partitionMetadata.rawSize = partition.getRawSize();
                  partitionMetadata.rowsCount = partition.getRowsCount();
                  partitionMetadata.filesCount = partition.getFilesCount();
                  partitionMetadata.isCompressed = partition.isCompressed();
                  outTable.partitions.add(partitionMetadata);
                }
              }
              String metadataText = HiveMetadataDumpFormat.MAPPER.writeValueAsString(outTable);
              synchronized (writer) {
                writer.write(metadataText);
                writer.write('\n');
              }
            } catch (Exception e) {
              // Failure to dump a single table should not prevent the rest of the tables from being
              // dumped.
              LOG.warn(
                  "Metadata cannot be dumped for table "
                      + databaseName
                      + "."
                      + tableName
                      + " due to an exception, skipping it.",
                  e);
            }
          });
    }

    @Override
    protected String toCallDescription() {
      return "get_all_databases()*.get_all_tables()*.[.get_fields(),.get_partition_keys()]";
    }
  }

  private static class FunctionsTask extends AbstractHiveMetadataTask implements FunctionsFormat {

    private FunctionsTask(@Nonnull Predicate<String> schemaPredicate) {
      super(ZIP_ENTRY_NAME, schemaPredicate);
    }

    @Override
    protected void run(@Nonnull Writer writer, @Nonnull ThriftClientHandle thriftClientHandle)
        throws Exception {
      try (HiveMetastoreThriftClient client =
              thriftClientHandle.newClient("functions-task-client");
          RecordProgressMonitor monitor =
              new RecordProgressMonitor("Writing functions to " + getTargetPath())) {
        try (CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer)) {
          for (Function function : client.getFunctions()) {
            if (isIncludedSchema(MoreObjects.firstNonNull(function.getDatabaseName(), ""))) {
              monitor.count();
              printer.printRecord(
                  function.getDatabaseName(),
                  function.getFunctionName(),
                  function.getType(),
                  function.getClassName(),
                  function.getOwner(),
                  function.getOwnerType(),
                  function.getCreateTime());
            }
          }
        }
      }
    }

    @Override
    protected String toCallDescription() {
      return "get_all_functions()";
    }
  }

  public HiveMetadataConnector() {
    super("hiveql");
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    Predicate<String> schemaPredicate = arguments.getSchemaPredicate();
    boolean shouldDumpPartitions =
        arguments.isHiveMetastorePartitionMetadataDumpingEnabled() || arguments.isAssessment();

    out.add(new SchemataTask(schemaPredicate));
    out.add(new TablesJsonTask(schemaPredicate, shouldDumpPartitions));
    out.add(new FunctionsTask(schemaPredicate));

    if (arguments.isAssessment()) {
      out.add(new DatabasesTask(schemaPredicate));
    }
  }
}
