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

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.time.Clock;
import java.util.List;
import javax.annotation.Nonnull;

/** Minimal skeleton Databricks metadata connector. */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from a Databricks workspace (skeleton connector).")
public class DatabricksConnector implements MetadataConnector {
  public static final String CONNECTOR_NAME = "databricks";

  @Nonnull
  @Override
  public String getName() {
    return CONNECTOR_NAME;
  }

  @Nonnull
  @Override
  public String getDefaultFileName(boolean isAssessment, Clock clock) {
    // Let MetadataConnector default behaviour handle naming; mirror other connectors if needed.
    return MetadataConnector.super.getDefaultFileName(isAssessment, clock);
  }

  @Override
  public void validate(@Nonnull ConnectorArguments arguments) {
    Preconditions.checkArgument(arguments.hasUri(), "--url param is required");
  }

  @Override
  public void addTasksTo(
      @Nonnull List<? super Task<?>> out,
      @Nonnull ConnectorArguments arguments)
      throws Exception {
    out.add(new DatabricksJsonlTask("catalogs.jsonl", DatabricksJsonlTask.Output.CATALOGS));
    out.add(new DatabricksJsonlTask("databases.jsonl", DatabricksJsonlTask.Output.DATABASES));
    out.add(new DatabricksJsonlTask("tables-raw.jsonl", DatabricksJsonlTask.Output.TABLES));
    out.add(new DumpMetadataTask(getName()));
    out.add(new FormatTask(getName()));
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    return new DatabricksHandle(
        new DatabricksClient(
            arguments.getUri(), arguments.getPasswordOrPrompt(), arguments.getWarehouse()));
  }

  @Nonnull
  @Override
  public Iterable<ConnectorProperty> getPropertyConstants() {
    return ImmutableList.of();
  }
}
