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
package com.google.edwmigration.dumper.application.dumper.connector.databricks;

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.DatabricksFilter.HIVE_METASTORE_CATALOG;

import com.databricks.sdk.WorkspaceClient;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentSchemaPredicate;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlRunner;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.DatabricksFilter;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.HiveMetastoreTasks;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.UnityCatalogTasks;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.CatalogsFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Databricks metadata connector. */
@AutoService(Connector.class)
@Description(
    "Dumps metadata from the Databricks Unity Catalog catalogs using the Unity Catalog API, and"
        + " from the hive_metastore catalog using a SQL warehouse or an all-purpose cluster.")
@RespectsInput(
    order = 300,
    arg = ConnectorArguments.OPT_WAREHOUSE,
    description =
        "The ID of the Databricks SQL warehouse that runs the metadata statements of the"
            + " hive_metastore catalog. Without it or --cluster, the hive_metastore catalog is"
            + " skipped, and only the Unity Catalog catalogs are dumped. The statements run with"
            + " the Databricks JDBC driver on Java 11 or above, and with the Statement Execution"
            + " API on Java 8, unless -Ddatabricks.metadata.warehouse-transport is set.")
@RespectsInput(
    order = 350,
    arg = ConnectorArguments.OPT_CLUSTER,
    description =
        "The ID of a running all-purpose cluster that runs the metadata statements of the"
            + " hive_metastore catalog instead of a SQL warehouse, such as the cluster of the"
            + " notebook that runs the dumper. The cluster must run Databricks Runtime 16.2 or"
            + " above; dedicated access mode is supported. Requires running the dumper with Java 11"
            + " or above.")
@RespectsInput(
    order = 600,
    arg = ConnectorArguments.OPT_DATABASE,
    description =
        "The list of catalogs to dump, separated by commas, such as main,hive_metastore. By"
            + " default, every catalog except the system, samples and __databricks_internal"
            + " catalogs.")
@RespectsArgumentSchemaPredicate
public class DatabricksMetadataConnector extends AbstractDatabricksConnector
    implements MetadataConnector {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksMetadataConnector.class);

  public static final String CONNECTOR_NAME = "databricks";

  // How many per-object calls, such as DESCRIBE TABLE statements, run at a time without
  // --thread-pool-size. 4 fits --cluster, whose metadata statements all run in the single driver
  // JVM of the cluster, and --warehouse over the Statement Execution API, whose executeStatement
  // calls aren't retried after transient HTTP failures.
  private static final int DEFAULT_PARALLELISM = 4;
  // --warehouse over JDBC runs 2x faster with 8 than with 4, and 8 stays below the ~10 concurrent
  // queries of a SQL warehouse cluster, beyond which queries queue or the warehouse scales out.
  private static final int DEFAULT_JDBC_WAREHOUSE_PARALLELISM = 8;

  /** Properties of the Databricks metadata connector, passed as {@code -D<name>=<value>}. */
  enum DatabricksMetadataProperty implements ConnectorProperty {
    WAREHOUSE_TRANSPORT("warehouse-transport", WAREHOUSE_TRANSPORT_DESCRIPTION);

    private final String name;
    private final String description;

    DatabricksMetadataProperty(String name, String description) {
      this.name = "databricks.metadata." + name;
      this.description = description;
    }

    @Override
    public String getName() {
      return this.name;
    }

    @Override
    public String getDescription() {
      return this.description;
    }
  }

  public DatabricksMetadataConnector() {
    super(CONNECTOR_NAME);
  }

  @Override
  protected Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return DatabricksMetadataProperty.class;
  }

  @Override
  ConnectorProperty warehouseTransportProperty() {
    return DatabricksMetadataProperty.WAREHOUSE_TRANSPORT;
  }

  @Override
  void validateForConnector(ConnectorArguments arguments) {
    if (getWarehouse(arguments) == null
        && getCluster(arguments) == null
        && !DatabricksFilter.fromArguments(arguments).acceptsUnityCatalog()) {
      throw new MetadataDumperUsageException(
          "The hive_metastore catalog needs a SQL warehouse or a cluster, because its metadata is"
              + " read with SQL statements. Please specify '--warehouse <warehouse-id>' or"
              + " '--cluster <cluster-id>'.");
    }

    int threadPoolSize = arguments.getThreadPoolSize();
    if (threadPoolSize < 1) {
      throw new MetadataDumperUsageException(
          String.format("The --thread-pool-size must be at least 1, but was %d.", threadPoolSize));
    }
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) {
    Preconditions.checkNotNull(out, "Tasks list was null.");
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");

    out.add(new DumpMetadataTask(arguments, DatabricksMetadataDumpFormat.FORMAT_NAME));
    out.add(new FormatTask(DatabricksMetadataDumpFormat.FORMAT_NAME));

    DatabricksFilter filter = DatabricksFilter.fromArguments(arguments);
    List<Task<?>> metadataTasks = new ArrayList<>();
    if (filter.acceptsUnityCatalog()) {
      metadataTasks.addAll(UnityCatalogTasks.create());
    }

    if (filter.acceptsCatalog(HIVE_METASTORE_CATALOG)) {
      if (getWarehouse(arguments) != null || getCluster(arguments) != null) {
        metadataTasks.addAll(HiveMetastoreTasks.create());
      } else {
        logger.warn(
            "Skipping the hive_metastore catalog because no SQL warehouse or cluster was given with"
                + " '--warehouse' or '--cluster'.");
      }
    }

    out.addAll(metadataTasks);

    // catalogs-hms.csv is written without a statement, so it doesn't count as a success.
    out.add(
        DatabricksHandle.createAllFailedCheckTask(
            ImmutableList.copyOf(
                Iterables.filter(
                    metadataTasks,
                    task -> !CatalogsFormat.HMS_ZIP_ENTRY_NAME.equals(task.getTargetPath())))));
  }

  @Override
  public Handle open(ConnectorArguments arguments) throws IOException {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    WorkspaceClient client = createWorkspaceClient(arguments);
    DatabricksFilter filter = DatabricksFilter.fromArguments(arguments);
    String specificationVersion = System.getProperty(JAVA_VERSION_PROPERTY);
    int parallelism = getParallelism(arguments, specificationVersion);
    DatabricksSqlRunner sqlRunner =
        filter.acceptsCatalog(HIVE_METASTORE_CATALOG)
            ? createSqlRunner(client, arguments, parallelism, specificationVersion)
            : null;
    return new DatabricksHandle(sqlRunner, client, filter, parallelism);
  }

  /**
   * Returns how many per-object calls, such as DESCRIBE TABLE statements, run at a time: the
   * --thread-pool-size if given, or else {@link #DEFAULT_JDBC_WAREHOUSE_PARALLELISM} when JDBC runs
   * the hive_metastore statements on a SQL warehouse, and {@link #DEFAULT_PARALLELISM} otherwise.
   * The default of --thread-pool-size, 32, would overload the driver of a small cluster.
   */
  private int getParallelism(ConnectorArguments arguments, String specificationVersion) {
    if (arguments.getOptions().has(ConnectorArguments.OPT_THREAD_POOL_SIZE)) {
      return arguments.getThreadPoolSize();
    }

    boolean jdbcOnWarehouse =
        getWarehouse(arguments) != null
            && DatabricksFilter.fromArguments(arguments).acceptsCatalog(HIVE_METASTORE_CATALOG)
            && getWarehouseTransport(arguments, specificationVersion) == WarehouseTransport.JDBC;
    return jdbcOnWarehouse ? DEFAULT_JDBC_WAREHOUSE_PARALLELISM : DEFAULT_PARALLELISM;
  }
}
