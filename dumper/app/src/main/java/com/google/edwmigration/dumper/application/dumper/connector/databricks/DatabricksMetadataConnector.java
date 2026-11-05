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
import static org.apache.commons.lang3.StringUtils.trimToNull;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.compute.ClusterDetails;
import com.databricks.sdk.service.compute.DataSecurityMode;
import com.databricks.sdk.service.compute.State;
import com.databricks.sdk.service.sql.Disposition;
import com.google.auto.service.AutoService;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentSchemaPredicate;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksClientSqlRunner;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlRunner;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.JdbcSqlRunner;
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
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Databricks metadata connector. */
@AutoService(Connector.class)
@Description(
    "Dumps metadata from the Databricks Unity Catalog catalogs using the Unity Catalog API, and"
        + " from the hive_metastore catalog using a SQL warehouse or an all-purpose cluster.")
@RespectsInput(
    order = 100,
    arg = ConnectorArguments.OPT_URI,
    description = "The Databricks workspace URL (e.g. https://<workspace-host>).")
@RespectsInput(
    order = 200,
    arg = ConnectorArguments.OPT_PASSWORD,
    description =
        "The Databricks personal access token (PAT) or OAuth token. Without it, the Databricks"
            + " SDK default authentication is used, such as the DATABRICKS_TOKEN environment"
            + " variable.")
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
public class DatabricksMetadataConnector extends AbstractConnector implements MetadataConnector {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksMetadataConnector.class);

  public static final String CONNECTOR_NAME = "databricks";

  private static final int MIN_JDBC_JAVA_MAJOR_VERSION = 11;
  private static final String JAVA_VERSION_PROPERTY = "java.specification.version";
  // How many per-object calls, such as DESCRIBE TABLE statements, run at a time without
  // --thread-pool-size. 4 fits --cluster, whose metadata statements all run in the single driver
  // JVM of the cluster, and --warehouse over the Statement Execution API, whose executeStatement
  // calls aren't retried after transient HTTP failures.
  private static final int DEFAULT_PARALLELISM = 4;
  // --warehouse over JDBC runs 2x faster with 8 than with 4, and 8 stays below the ~10 concurrent
  // queries of a SQL warehouse cluster, beyond which queries queue or the warehouse scales out.
  private static final int DEFAULT_JDBC_WAREHOUSE_PARALLELISM = 8;
  private static final ImmutableSet<DataSecurityMode> DEDICATED_ACCESS_MODES =
      ImmutableSet.of(
          DataSecurityMode.SINGLE_USER,
          DataSecurityMode.DATA_SECURITY_MODE_DEDICATED,
          DataSecurityMode.LEGACY_SINGLE_USER);

  /** How SQL statement results are fetched from {@code --warehouse}. */
  private enum WarehouseTransport {
    EXTERNAL_LINKS("external-links", Disposition.EXTERNAL_LINKS),
    INLINE("inline", Disposition.INLINE),
    JDBC("jdbc", null);

    private final String value;
    // The disposition of the Statement Execution API, or null for the JDBC driver.
    @Nullable private final Disposition disposition;

    WarehouseTransport(String value, @Nullable Disposition disposition) {
      this.value = value;
      this.disposition = disposition;
    }

    /** Returns the transport of {@code rawValue}, ignoring case and surrounding whitespace. */
    static WarehouseTransport parse(String rawValue) {
      String trimmed = Strings.nullToEmpty(rawValue).trim();
      for (WarehouseTransport transport : values()) {
        if (transport.value.equalsIgnoreCase(trimmed)) {
          return transport;
        }
      }

      throw new MetadataDumperUsageException(
          String.format(
              "Invalid value '%s' for '%s'. Allowed values: '%s'.",
              rawValue,
              DatabricksMetadataProperty.WAREHOUSE_TRANSPORT.getName(),
              Joiner.on("', '").join(values())));
    }

    /** Returns the value that {@link #parse} accepts, such as {@code external-links}. */
    @Override
    public String toString() {
      return this.value;
    }
  }

  /** Properties of the Databricks metadata connector, passed as {@code -D<name>=<value>}. */
  enum DatabricksMetadataProperty implements ConnectorProperty {
    WAREHOUSE_TRANSPORT(
        "warehouse-transport",
        "How SQL statement results are fetched from '--warehouse': 'jdbc' (Databricks JDBC driver"
            + " over Thrift without external links; requires Java 11 or above), 'inline'"
            + " (Statement Execution API without external links, up to 25 MiB per statement), or"
            + " 'external-links' (Statement Execution API with pre-signed cloud-storage links)."
            + " Default: 'jdbc' on Java 11 or above, 'external-links' on Java 8.");

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
  public void validate(ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    validateNotAssessment(getName(), arguments);
    requireUrl(arguments);
    String warehouseId = getWarehouse(arguments);
    String clusterId = getCluster(arguments);
    if (warehouseId != null && clusterId != null) {
      throw new MetadataDumperUsageException(
          "The hive_metastore statements run on either a SQL warehouse or a cluster. Please"
              + " specify either '--warehouse <warehouse-id>' or '--cluster <cluster-id>', not"
              + " both.");
    }

    if (arguments.isDefinitionSpecified(DatabricksMetadataProperty.WAREHOUSE_TRANSPORT)) {
      if (warehouseId == null) {
        throw new MetadataDumperUsageException(
            String.format(
                "Option '%s' requires '--warehouse'.",
                DatabricksMetadataProperty.WAREHOUSE_TRANSPORT.getName()));
      }

      WarehouseTransport.parse(
          arguments.getDefinition(DatabricksMetadataProperty.WAREHOUSE_TRANSPORT));
    }

    if (warehouseId == null
        && clusterId == null
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
  private static int getParallelism(ConnectorArguments arguments, String specificationVersion) {
    if (arguments.getOptions().has(ConnectorArguments.OPT_THREAD_POOL_SIZE)) {
      return arguments.getThreadPoolSize();
    }

    boolean jdbcOnWarehouse =
        getWarehouse(arguments) != null
            && DatabricksFilter.fromArguments(arguments).acceptsCatalog(HIVE_METASTORE_CATALOG)
            && getWarehouseTransport(arguments, specificationVersion) == WarehouseTransport.JDBC;
    return jdbcOnWarehouse ? DEFAULT_JDBC_WAREHOUSE_PARALLELISM : DEFAULT_PARALLELISM;
  }

  /**
   * Returns the transport of -Ddatabricks.metadata.warehouse-transport if given, or else JDBC on
   * Java 11 or above, whose persistent sessions run the statements 1.7x-1.9x faster without
   * external links, and the external links of the Statement Execution API on Java 8, which can't
   * load the JDBC driver.
   */
  private static WarehouseTransport getWarehouseTransport(
      ConnectorArguments arguments, String specificationVersion) {
    // Like validate(), treats an empty value as not given.
    if (arguments.isDefinitionSpecified(DatabricksMetadataProperty.WAREHOUSE_TRANSPORT)) {
      return WarehouseTransport.parse(
          arguments.getDefinition(DatabricksMetadataProperty.WAREHOUSE_TRANSPORT));
    }

    return isJava11OrAbove(specificationVersion)
        ? WarehouseTransport.JDBC
        : WarehouseTransport.EXTERNAL_LINKS;
  }

  /**
   * Returns whether a java.specification.version is 11 or above. It is 1.8 on Java 8, which isn't a
   * number, and a number, such as 11 or 21, on Java 9 and above. Returns false when the version
   * can't be read.
   */
  private static boolean isJava11OrAbove(String specificationVersion) {
    try {
      return Integer.parseInt(Strings.nullToEmpty(specificationVersion).trim())
          >= MIN_JDBC_JAVA_MAJOR_VERSION;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  /**
   * Returns the SQL runner of the SQL warehouse of --warehouse or of the cluster of --cluster, or
   * null when neither was given. Throws a usage error when JDBC is selected on Java 8 or when the
   * cluster can't run the statements, and an IOException when a JDBC runner can't open a
   * connection.
   */
  private static DatabricksSqlRunner createSqlRunner(
      WorkspaceClient client,
      ConnectorArguments arguments,
      int parallelism,
      String specificationVersion)
      throws IOException {
    String warehouseId = getWarehouse(arguments);
    if (warehouseId != null) {
      WarehouseTransport transport = getWarehouseTransport(arguments, specificationVersion);
      // The defaults depend on the Java version, so the log shows the transport and parallelism.
      logger.info(
          "Running the hive_metastore statements on SQL warehouse {} with the '{}' transport, {}"
              + " at a time.",
          warehouseId,
          transport,
          parallelism);
      return transport == WarehouseTransport.JDBC
          ? JdbcSqlRunner.forWarehouse(client, warehouseId, parallelism)
          : new DatabricksClientSqlRunner(client, warehouseId, transport.disposition);
    }

    String clusterId = getCluster(arguments);
    if (clusterId == null) {
      return null;
    }

    // Java 8 fails before the cluster is checked, so that a stopped cluster isn't started in vain.
    if (!isJava11OrAbove(specificationVersion)) {
      throw new MetadataDumperUsageException(
          String.format(
              "The '--cluster' flag requires Java 11 or above, but the dumper is running on Java"
                  + " %s. Please run the dumper with Java 11 or above, or use '--warehouse"
                  + " <warehouse-id>' on Java 8.",
              specificationVersion));
    }

    // The runner opens a connection, so it is created after the cluster is checked.
    checkCluster(client.clusters().get(clusterId));
    return JdbcSqlRunner.forCluster(client, clusterId, parallelism);
  }

  /**
   * Throws a usage error when a cluster isn't running, so opening a JDBC connection doesn't trigger
   * an unintended cluster start, and warns when its access mode isn't dedicated.
   */
  private static void checkCluster(ClusterDetails cluster) {
    if (cluster.getState() != State.RUNNING) {
      throw new MetadataDumperUsageException(
          String.format(
              "The cluster %s is %s. Please start it, or run the dumper from a notebook attached to"
                  + " it.",
              cluster.getClusterId(), cluster.getState()));
    }

    if (!DEDICATED_ACCESS_MODES.contains(cluster.getDataSecurityMode())) {
      logger.warn(
          "The cluster {} uses the access mode {}. Only dedicated access mode was tried so far.",
          cluster.getClusterId(),
          cluster.getDataSecurityMode());
    }
  }

  private static void validateNotAssessment(String connectorName, ConnectorArguments arguments) {
    if (arguments.isAssessment()) {
      throw new MetadataDumperUsageException(
          String.format(
              "The '--assessment' flag is not supported by the '%s' connector. "
                  + "Databricks hardware telemetry and DBU usage assessment are collected via the "
                  + "Databricks Discovery Notebook.",
              connectorName));
    }
  }

  private static String requireUrl(ConnectorArguments arguments) {
    String url = arguments.getUri();
    if (url == null || url.trim().isEmpty()) {
      throw new MetadataDumperUsageException(
          "Required workspace URL was not provided. Please specify '--url <https://workspace-host>'.");
    }
    return url.trim();
  }

  /** Returns the ID of the SQL warehouse of --warehouse, or null if none. */
  private static String getWarehouse(ConnectorArguments arguments) {
    return trimToNull(arguments.getWarehouse());
  }

  /** Returns the ID of the cluster of --cluster, or null if none. */
  private static String getCluster(ConnectorArguments arguments) {
    return trimToNull(arguments.getCluster());
  }

  /**
   * Creates the client of the workspace of --url. Without --password, the client uses the
   * Databricks SDK default authentication, such as the DATABRICKS_TOKEN environment variable.
   */
  private static WorkspaceClient createWorkspaceClient(ConnectorArguments arguments) {
    DatabricksConfig config = new DatabricksConfig().setHost(requireUrl(arguments));
    if (arguments.isPasswordFlagProvided()) {
      String token = arguments.getPasswordOrPrompt().trim();
      if (!token.isEmpty()) {
        config.setToken(token);
      }
    }

    return new WorkspaceClient(config);
  }
}
