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

import static com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.commons.lang3.StringUtils.trimToNull;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.compute.ClusterDetails;
import com.databricks.sdk.service.compute.DataSecurityMode;
import com.databricks.sdk.service.compute.State;
import com.databricks.sdk.service.sql.Disposition;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksClientSqlRunner;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlRunner;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.JdbcSqlRunner;
import java.io.IOException;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class of the Databricks connectors: the --url and --password arguments, the checks of the
 * arguments that the connectors share, the workspace client, and the SQL runner of --warehouse or
 * --cluster.
 */
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
abstract class AbstractDatabricksConnector extends AbstractConnector {

  private static final Logger logger = LoggerFactory.getLogger(AbstractDatabricksConnector.class);

  private static final int MIN_JDBC_JAVA_MAJOR_VERSION = 11;
  static final String JAVA_VERSION_PROPERTY = "java.specification.version";
  static final String WAREHOUSE_TRANSPORT_DESCRIPTION =
      "How SQL statement results are fetched from '--warehouse': 'jdbc' (Databricks JDBC driver"
          + " over Thrift without external links; requires Java 11 or above), 'inline'"
          + " (Statement Execution API without external links, up to 25 MiB per statement), or"
          + " 'external-links' (Statement Execution API with pre-signed cloud-storage links)."
          + " Default: 'jdbc' on Java 11 or above, 'external-links' on Java 8.";
  private static final ImmutableSet<DataSecurityMode> DEDICATED_ACCESS_MODES =
      ImmutableSet.of(
          DataSecurityMode.SINGLE_USER,
          DataSecurityMode.DATA_SECURITY_MODE_DEDICATED,
          DataSecurityMode.LEGACY_SINGLE_USER);

  /** How SQL statement results are fetched from {@code --warehouse}. */
  enum WarehouseTransport {
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

    /** Returns the value of the property, such as {@code external-links}. */
    @Override
    public String toString() {
      return this.value;
    }
  }

  AbstractDatabricksConnector(String name) {
    super(name);
  }

  /** Returns the property of this connector that selects the {@link WarehouseTransport}. */
  abstract ConnectorProperty warehouseTransportProperty();

  /**
   * Checks the arguments that the Databricks connectors share, and then the arguments of this
   * connector.
   */
  @Override
  public final void validate(ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    validateNotAssessment(arguments);
    requireUrl(arguments);
    validateComputeAndTransport(arguments);
    validateForConnector(arguments);
  }

  /**
   * Checks the arguments of this connector. Called by {@link #validate} after the checks of the
   * arguments that the Databricks connectors share.
   */
  abstract void validateForConnector(ConnectorArguments arguments);

  private void validateNotAssessment(ConnectorArguments arguments) {
    if (arguments.isAssessment()) {
      throw new MetadataDumperUsageException(
          String.format(
              "The '--assessment' flag is not supported by the '%s' connector. "
                  + "Databricks hardware telemetry and DBU usage assessment are collected via the "
                  + "Databricks Discovery Notebook.",
              getName()));
    }
  }

  private void validateComputeAndTransport(ConnectorArguments arguments) {
    String warehouseId = getWarehouse(arguments);
    if (warehouseId != null && getCluster(arguments) != null) {
      throw new MetadataDumperUsageException(
          "SQL statements run on either a SQL warehouse or a cluster. Please specify either"
              + " '--warehouse <warehouse-id>' or '--cluster <cluster-id>', not both.");
    }

    ConnectorProperty transportProperty = warehouseTransportProperty();
    if (parseProperty(arguments, transportProperty, WarehouseTransport.values()) != null
        && warehouseId == null) {
      throw new MetadataDumperUsageException(
          String.format("Option '%s' requires '--warehouse'.", transportProperty.getName()));
    }
  }

  /**
   * Returns the value of {@code property} among {@code values}, which are matched by their
   * toString() ignoring case and surrounding whitespace, or null if the property isn't set or is
   * empty.
   */
  @Nullable
  static <T> T parseProperty(ConnectorArguments arguments, ConnectorProperty property, T[] values) {
    if (!arguments.isDefinitionSpecified(property)) {
      return null;
    }

    String rawValue = arguments.getDefinition(property);
    for (T value : values) {
      if (value.toString().equalsIgnoreCase(rawValue.trim())) {
        return value;
      }
    }

    throw new MetadataDumperUsageException(
        String.format(
            "Invalid value '%s' for '%s'. Allowed values: '%s'.",
            rawValue, property.getName(), Joiner.on("', '").join(values)));
  }

  /**
   * Returns the transport of {@link #warehouseTransportProperty()} if given, or else JDBC on Java
   * 11 or above, whose persistent sessions run the statements 1.7x-1.9x faster without external
   * links, and the external links of the Statement Execution API on Java 8, which can't load the
   * JDBC driver.
   */
  WarehouseTransport getWarehouseTransport(
      ConnectorArguments arguments, String specificationVersion) {
    return firstNonNull(
        parseProperty(arguments, warehouseTransportProperty(), WarehouseTransport.values()),
        isJava11OrAbove(specificationVersion)
            ? WarehouseTransport.JDBC
            : WarehouseTransport.EXTERNAL_LINKS);
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
  DatabricksSqlRunner createSqlRunner(
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
          "Running SQL statements on SQL warehouse {} with the '{}' transport, {} at a time.",
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
  @VisibleForTesting
  static void checkCluster(ClusterDetails cluster) {
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

  static String requireUrl(ConnectorArguments arguments) {
    String url = arguments.getUri();
    if (url == null || url.trim().isEmpty()) {
      throw new MetadataDumperUsageException(
          "Required workspace URL was not provided. Please specify '--url <https://workspace-host>'.");
    }
    return url.trim();
  }

  /** Returns the ID of the SQL warehouse of --warehouse, or null if none. */
  static String getWarehouse(ConnectorArguments arguments) {
    return trimToNull(arguments.getWarehouse());
  }

  /** Returns the ID of the cluster of --cluster, or null if none. */
  static String getCluster(ConnectorArguments arguments) {
    return trimToNull(arguments.getCluster());
  }

  /**
   * Creates the client of the workspace of --url. Without --password, the client uses the
   * Databricks SDK default authentication, such as the DATABRICKS_TOKEN environment variable.
   */
  static WorkspaceClient createWorkspaceClient(ConnectorArguments arguments) {
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
