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
package com.google.edwmigration.dumper.application.dumper.connector.meta;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.ConnectorRepository;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Metaconnector is a connector that calls one or more other connectors and collects their results
 * in the single zip file.
 */
public abstract class AbstractMetaConnector implements Connector {

  private final String name;
  private final String format;
  private final ImmutableList<String> underlyingConnectors;

  public AbstractMetaConnector(String name, String format, List<String> underlyingConnectors) {
    this.name = name;
    this.format = format;
    this.underlyingConnectors = ImmutableList.copyOf(underlyingConnectors);
  }

  @Nonnull
  @Override
  public String getName() {
    return name;
  }

  @Nonnull
  @Override
  public String getDefaultFileName(boolean isAssessment, Clock clock) {
    return ArchiveNameUtil.getFileName(name);
  }

  @Override
  public void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception {
    out.add(new DumpMetadataTask(arguments, format));
    out.add(new FormatTask(format));
    for (String connectorName : underlyingConnectors) {
      ChildConnector childConnector = getChildConnector(connectorName);
      childConnector.createInitializerTask().ifPresent(out::add);
      List<Task<?>> tasks = new ArrayList<>();
      childConnector.addTasksTo(tasks, arguments);
      tasks.stream()
          .map(task -> new AsMetaConnectorTask<>(task, connectorName, name))
          .forEach(out::add);
    }
  }

  private ChildConnector getChildConnector(String connectorName) {
    Connector connector = ConnectorRepository.getInstance().getByName(connectorName);
    checkState(
        connector instanceof ChildConnector,
        "The connector '%s' of class '%s' cannot be used as a child connector in the metaconnector.",
        connectorName,
        connector.getClass().getCanonicalName());
    return (ChildConnector) connector;
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    return new MetaHandle();
  }
}
