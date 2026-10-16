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
import com.databricks.sdk.core.DatabricksConfig;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentSchemaPredicate;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper;
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
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Databricks metadata connector. */
@AutoService(Connector.class)
@Description(
    "Dumps metadata from the Databricks Unity Catalog catalogs using the Unity Catalog API, and"
        + " from the hive_metastore catalog using a SQL warehouse.")
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
            + " hive_metastore catalog. Without it, the hive_metastore catalog is skipped, and only"
            + " the Unity Catalog catalogs are dumped.")
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

  public DatabricksMetadataConnector() {
    super(CONNECTOR_NAME);
  }

  @Override
  public void validate(ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    validateNotAssessment(getName(), arguments);
    requireUrl(arguments);
    if (findWarehouse(arguments) == null
        && !DatabricksFilter.fromArguments(arguments).acceptsUnityCatalog()) {
      throw new MetadataDumperUsageException(
          "The hive_metastore catalog needs a SQL warehouse, because its metadata is read with SQL"
              + " statements. Please specify '--warehouse <warehouse-id>'.");
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
      if (findWarehouse(arguments) != null) {
        metadataTasks.addAll(HiveMetastoreTasks.create());
      } else {
        logger.warn(
            "Skipping the hive_metastore catalog because no SQL warehouse was given with"
                + " '--warehouse'.");
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
  public Handle open(ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    WorkspaceClient client = createWorkspaceClient(arguments);
    String warehouseId = findWarehouse(arguments);
    DatabricksSqlHelper sqlHelper =
        warehouseId == null ? null : new DatabricksSqlHelper(client, warehouseId);
    return new DatabricksHandle(sqlHelper, client, DatabricksFilter.fromArguments(arguments));
  }

  static void validateNotAssessment(String connectorName, ConnectorArguments arguments) {
    if (arguments.isAssessment()) {
      throw new MetadataDumperUsageException(
          String.format(
              "The '--assessment' flag is not supported by the '%s' connector. "
                  + "Databricks hardware telemetry and DBU usage assessment are collected via the "
                  + "Databricks Discovery Notebook.",
              connectorName));
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
  static String findWarehouse(ConnectorArguments arguments) {
    String warehouse = arguments.getWarehouse();
    return warehouse == null || warehouse.trim().isEmpty() ? null : warehouse.trim();
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
