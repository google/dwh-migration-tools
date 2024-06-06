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
package com.google.edwmigration.dumper.application.dumper.connector.hive;

import com.google.auto.service.AutoService;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabasePredicate;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.ext.hive.metastore.Database;
import com.google.edwmigration.dumper.ext.hive.metastore.DelegationToken;
import com.google.edwmigration.dumper.ext.hive.metastore.Field;
import com.google.edwmigration.dumper.ext.hive.metastore.Function;
import com.google.edwmigration.dumper.ext.hive.metastore.HiveMetastoreThriftClient;
import com.google.edwmigration.dumper.ext.hive.metastore.Partition;
import com.google.edwmigration.dumper.ext.hive.metastore.PartitionKey;
import com.google.edwmigration.dumper.ext.hive.metastore.Table;
import com.google.edwmigration.dumper.ext.hive.metastore.ThriftJsonSerializer;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.ConcurrentProgressMonitor;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.ConcurrentRecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.HiveMetadataDumpFormat;
import com.google.gson.stream.JsonWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RespectsArgumentDatabasePredicate
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from the Hive metastore via Thrift.")
public class HiveMetadataConnector extends AbstractHiveConnector
    implements HiveMetadataDumpFormat, MetadataConnector {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetadataConnector.class);

  private abstract static class AbstractHiveMetadataTask extends AbstractHiveTask {

    private final Predicate<String> databasePredicate;

    private AbstractHiveMetadataTask(
        String targetPath, @Nonnull Predicate<String> databasePredicate) {
      super(targetPath);
      this.databasePredicate =
          Preconditions.checkNotNull(databasePredicate, "Database predicate was null.");
    }

    private AbstractHiveMetadataTask(String targetPath) {
      this(targetPath, unused -> true);
    }

    protected boolean isIncludedDatabase(@Nonnull String database) {
      return databasePredicate.test(database);
    }
  }

  private static class SchemataTask extends AbstractHiveMetadataTask implements SchemataFormat {

    private SchemataTask(@Nonnull Predicate<String> databasePredicate) {
      super(ZIP_ENTRY_NAME, databasePredicate);
    }

    @Override
    protected void run(@Nonnull Writer writer, @Nonnull ThriftClientHandle thriftClientHandle)
        throws Exception {
      try (HiveMetastoreThriftClient client = thriftClientHandle.newClient("schemata-task-client");
          RecordProgressMonitor monitor =
              new RecordProgressMonitor("Writing database names to " + getTargetPath());
          CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer)) {
        ImmutableList<String> allDatabases = client.getAllDatabaseNames();
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

    private DatabasesTask(@Nonnull Predicate<String> databasePredicate) {
      super(ZIP_ENTRY_NAME, databasePredicate);
    }

    @Override
    protected void run(@Nonnull Writer writer, @Nonnull ThriftClientHandle thriftClientHandle)
        throws Exception {
      try (HiveMetastoreThriftClient client =
              thriftClientHandle.newClient("databases-task-client");
          RecordProgressMonitor monitor =
              new RecordProgressMonitor("Writing databases info to " + getTargetPath());
          CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer)) {
        ImmutableList<String> allDatabases = client.getAllDatabaseNames();
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

  private static class DatabasesJsonlTask extends HiveJsonlTask {

    private DatabasesJsonlTask() {
      super("databases.jsonl", "get_all_databases()*.get_database()");
    }

    @Override
    protected ImmutableList<? extends TBase<?, ?>> retrieveEntities(
        HiveMetastoreThriftClient client) throws Exception {
      return client.getRawDatabases();
    }
  }

  private static class TablesJsonTask extends AbstractHiveMetadataTask
      implements TablesJsonTaskFormat {

    private final boolean isHiveMetastorePartitionMetadataDumpingEnabled;

    private TablesJsonTask(
        @Nonnull Predicate<String> databasePredicate,
        boolean isHiveMetastorePartitionMetadataDumpingEnabled) {
      super(ZIP_ENTRY_NAME, databasePredicate);
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
              ImmutableList<String> allDatabases = thriftClient.getAllDatabaseNames();
              for (String databaseName : allDatabases) {
                if (isIncludedDatabase(databaseName)) {
                  ImmutableList<String> allTables =
                      thriftClient.getAllTableNamesInDatabase(databaseName);
                  try (ConcurrentProgressMonitor monitor =
                      new ConcurrentRecordProgressMonitor(
                          "Writing tables in database '" + databaseName + "' to " + getTargetPath(),
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
              outTable.viewText = table.getExpandedViewText();
              outTable.location = table.getLocation();
              outTable.lastDdlTime = table.getLastDdlTime();
              outTable.totalSize = table.getTotalSize();
              outTable.rawSize = table.getRawSize();
              outTable.rowsCount = table.getRowsCount();
              outTable.filesCount = table.getFilesCount();
              outTable.retention = table.getRetention();
              outTable.bucketsCount = table.getBucketsCount();
              outTable.isCompressed = table.isCompressed();

              outTable.serializationLib = table.getSerializationLib();
              outTable.inputFormat = table.getInputFormat();
              outTable.outputFormat = table.getOutputFormat();

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

  private static class TablesRawJsonlTask extends AbstractHiveMetadataTask {

    private TablesRawJsonlTask(Predicate<String> databasePredicate) {
      super("tables-raw.jsonl", databasePredicate);
    }

    @Override
    protected void run(@Nonnull Writer writer, @Nonnull ThriftClientHandle thriftClientHandle)
        throws Exception {
      try (ThriftClientPool clientPool =
          thriftClientHandle.newMultiThreadedThriftClientPool("tables-raw-task-pooled-client")) {
        clientPool.execute(
            thriftClient -> {
              ImmutableList<String> allDatabases = thriftClient.getAllDatabaseNames();
              for (String databaseName : allDatabases) {
                if (isIncludedDatabase(databaseName)) {
                  ImmutableList<String> allTables =
                      thriftClient.getAllTableNamesInDatabase(databaseName);
                  try (ConcurrentProgressMonitor monitor =
                      new ConcurrentRecordProgressMonitor(
                          "Writing tables in database '" + databaseName + "' to " + getTargetPath(),
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
              TBase<?, ?> rawTableThriftObject = table.getRawThriftObject();
              ImmutableMap<String, ImmutableList<? extends TBase<?, ?>>> additionalMetadata =
                  ImmutableMap.of(
                      "primaryKeys", table.getRawPrimaryKeys(),
                      "foreignKeys", table.getRawForeignKeys(),
                      "uniqueConstraints", table.getRawUniqueConstraints(),
                      "nonNullConstraints", table.getRawNonNullConstraints(),
                      "defaultConstraints", table.getRawDefaultConstraints(),
                      "checkConstraints", table.getRawCheckConstraints(),
                      "tableStatistics", table.getRawTableStatistics());
              ThriftJsonSerializer jsonSerializer = new ThriftJsonSerializer();
              synchronized (writer) {
                writer.write("{\"table\":");
                writer.write(jsonSerializer.serialize(rawTableThriftObject));
                for (Map.Entry<String, ImmutableList<? extends TBase<?, ?>>> entry :
                    additionalMetadata.entrySet()) {
                  writer.write(",\"");
                  writer.write(entry.getKey());
                  writer.write("\":");
                  jsonSerializer.serialize(entry.getValue(), writer);
                }
                writer.write("}");
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
      return "get_all_databases()*.get_all_tables()";
    }
  }

  private static class PartitionsJsonlTask extends AbstractHiveMetadataTask {

    private PartitionsJsonlTask(Predicate<String> databasePredicate) {
      super("partitions.jsonl", databasePredicate);
    }

    @Override
    protected void run(@Nonnull Writer writer, @Nonnull ThriftClientHandle thriftClientHandle)
        throws Exception {
      try (ThriftClientPool clientPool =
          thriftClientHandle.newMultiThreadedThriftClientPool("partitions-task-pooled-client")) {
        clientPool.execute(
            thriftClient -> {
              ImmutableList<String> allDatabases = thriftClient.getAllDatabaseNames();
              for (String databaseName : allDatabases) {
                if (isIncludedDatabase(databaseName)) {
                  ImmutableList<String> allTables =
                      thriftClient.getAllTableNamesInDatabase(databaseName);
                  try (ConcurrentProgressMonitor monitor =
                      new ConcurrentRecordProgressMonitor(
                          "Writing partitions of tables in database '"
                              + databaseName
                              + "' to "
                              + getTargetPath(),
                          allTables.size())) {
                    for (String tableName : allTables) {
                      dumpPartitions(monitor, writer, clientPool, databaseName, tableName);
                    }
                  }
                }
              }
            });
      }
    }

    private void dumpPartitions(
        @Nonnull ConcurrentProgressMonitor monitor,
        @Nonnull Writer writer,
        @Nonnull ThriftClientPool clientPool,
        @Nonnull String databaseName,
        @Nonnull String tableName) {
      clientPool.execute(
          (thriftClient) -> {
            try {
              monitor.count();
              ImmutableList<? extends TBase<?, ?>> partitions =
                  thriftClient.getTable(databaseName, tableName).getRawPartitions();
              ThriftJsonSerializer jsonSerializer = new ThriftJsonSerializer();
              synchronized (writer) {
                JsonWriter jsonWriter = new JsonWriter(writer);
                jsonWriter.setLenient(true);
                writer.write("{\"databaseName\":");
                jsonWriter.value(databaseName);
                writer.write(",\"tableName\":");
                jsonWriter.value(tableName);
                writer.write(",\"partitions\":");
                jsonSerializer.serialize(partitions, writer);
                writer.write("}\n");
              }
            } catch (Exception e) {
              // Failure to dump a single table should not prevent the rest of the tables from being
              // dumped.
              LOG.warn(
                  "Partitions cannot be dumped for table "
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
      return "get_all_databases()*.get_all_tables()*.get_partitions()";
    }
  }

  private static class FunctionsTask extends AbstractHiveMetadataTask implements FunctionsFormat {

    private FunctionsTask(@Nonnull Predicate<String> databasePredicate) {
      super(ZIP_ENTRY_NAME, databasePredicate);
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
            if (isIncludedDatabase(MoreObjects.firstNonNull(function.getDatabaseName(), ""))) {
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

  private static class FunctionsJsonlTask extends HiveJsonlTask {

    private FunctionsJsonlTask() {
      super("functions.jsonl", "get_all_functions()");
    }

    @Override
    protected ImmutableList<? extends TBase<?, ?>> retrieveEntities(
        HiveMetastoreThriftClient client) throws Exception {
      return client.getRawFunctions();
    }
  }

  private static class CatalogsJsonlTask extends HiveJsonlTask {

    private CatalogsJsonlTask() {
      super("catalogs.jsonl", "get_catalogs()*.get_catalog()");
    }

    @Override
    protected ImmutableList<? extends TBase<?, ?>> retrieveEntities(
        HiveMetastoreThriftClient client) throws Exception {
      return client.getRawCatalogs();
    }
  }

  private static class MasterKeysTask extends AbstractHiveMetadataTask {
    private static final CSVFormat MASTER_KEYS_FORMAT =
        FORMAT.builder().setHeader("MasterKey").build();

    private MasterKeysTask() {
      super("master-keys.csv");
    }

    @Override
    protected void run(@Nonnull Writer writer, @Nonnull ThriftClientHandle thriftClientHandle)
        throws Exception {
      try (HiveMetastoreThriftClient client =
              thriftClientHandle.newClient("master-keys-task-client");
          RecordProgressMonitor monitor =
              new RecordProgressMonitor("Writing Master Keys to " + getTargetPath())) {
        ImmutableList<String> masterKeys = client.getMasterKeys();
        try (CSVPrinter printer = MASTER_KEYS_FORMAT.print(writer)) {
          for (String masterKey : masterKeys) {
            monitor.count();
            printer.printRecord(masterKey);
          }
        }
      }
    }

    @Override
    public TaskCategory getCategory() {
      return TaskCategory.OPTIONAL;
    }

    @Override
    protected String toCallDescription() {
      return "get_master_keys()";
    }
  }

  private static class DelegationTokensTask extends AbstractHiveMetadataTask {
    private static final CSVFormat DELEGATION_TOKENS_FORMAT =
        FORMAT.builder().setHeader("Identifier", "Token").build();

    private DelegationTokensTask() {
      super("delegation-tokens.csv");
    }

    @Override
    protected void run(@Nonnull Writer writer, @Nonnull ThriftClientHandle thriftClientHandle)
        throws Exception {
      try (HiveMetastoreThriftClient client =
              thriftClientHandle.newClient("delegation-tokens-task-client");
          RecordProgressMonitor monitor =
              new RecordProgressMonitor("Writing Delegation Tokens to " + getTargetPath())) {
        ImmutableList<DelegationToken> delegationTokens = client.getDelegationTokens();
        try (CSVPrinter printer = DELEGATION_TOKENS_FORMAT.print(writer)) {
          for (DelegationToken delegationToken : delegationTokens) {
            monitor.count();
            printer.printRecord(delegationToken.identifier(), delegationToken.token());
          }
        }
      }
    }

    @Override
    public TaskCategory getCategory() {
      return TaskCategory.OPTIONAL;
    }

    @Override
    protected String toCallDescription() {
      return "get_all_token_identifiers()*.get_token()";
    }
  }

  private static class ResourcePlansJsonlTask extends HiveJsonlTask {

    private ResourcePlansJsonlTask() {
      super("resource-plans.jsonl", "get_all_resource_plans()");
    }

    @Override
    protected ImmutableList<? extends TBase<?, ?>> retrieveEntities(
        HiveMetastoreThriftClient client) throws Exception {
      return client.getRawResourcePlans();
    }
  }

  private abstract static class HiveJsonlTask extends AbstractHiveMetadataTask {
    private final String callDescription;

    private HiveJsonlTask(String targetPath, String callDescription) {
      super(targetPath);
      this.callDescription = callDescription;
    }

    @Override
    protected void run(@Nonnull Writer writer, @Nonnull ThriftClientHandle thriftClientHandle)
        throws Exception {
      try (HiveMetastoreThriftClient client =
              thriftClientHandle.newClient(getTargetPath() + "-task-client");
          RecordProgressMonitor monitor =
              new RecordProgressMonitor("Writing to " + getTargetPath())) {
        ThriftJsonSerializer serializer = new ThriftJsonSerializer();
        for (TBase<?, ?> entity : retrieveEntities(client)) {
          monitor.count();
          writer.write(serializer.serialize(entity));
          writer.write('\n');
        }
      }
    }

    protected abstract ImmutableList<? extends TBase<?, ?>> retrieveEntities(
        HiveMetastoreThriftClient client) throws Exception;

    @Override
    public TaskCategory getCategory() {
      return TaskCategory.OPTIONAL;
    }

    @Override
    protected String toCallDescription() {
      return callDescription;
    }
  }

  public HiveMetadataConnector() {
    super("hiveql");
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    Predicate<String> databasePredicate = arguments.getDatabasePredicate();
    boolean shouldDumpPartitions =
        arguments.isHiveMetastorePartitionMetadataDumpingEnabled() || arguments.isAssessment();

    out.add(new SchemataTask(databasePredicate));
    out.add(new FunctionsTask(databasePredicate));
    if (BooleanUtils.toBoolean(
        arguments.getDefinitionOrDefault(HiveConnectorProperty.MIGRATION_METADATA))) {
      out.add(new CatalogsJsonlTask());
      out.add(new DatabasesJsonlTask());
      out.add(new MasterKeysTask());
      out.add(new DelegationTokensTask());
      out.add(new FunctionsJsonlTask());
      out.add(new ResourcePlansJsonlTask());
      out.add(new TablesRawJsonlTask(databasePredicate));
      out.add(new PartitionsJsonlTask(databasePredicate));
    } else {
      out.add(new TablesJsonTask(databasePredicate, shouldDumpPartitions));
    }

    if (arguments.isAssessment()) {
      out.add(new DatabasesTask(databasePredicate));
    }
  }
}
