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
package com.google.edwmigration.dumper.application.dumper;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import java.util.ServiceLoader;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ConnectorRepository {

  private static class Inner {
    private static final ConnectorRepository INSTANCE = new ConnectorRepository();
  }

  public static ConnectorRepository getInstance() {
    return Inner.INSTANCE;
  }

  private final ImmutableMap<String, Connector> connectors;

  private ConnectorRepository() {
    ImmutableMap.Builder<String, Connector> builder = ImmutableMap.builder();
    for (Connector connector : ServiceLoader.load(Connector.class)) {
      builder.put(connector.getName().toLowerCase(), connector);
    }
    connectors = builder.build();
  }

  ImmutableSet<String> getAllNames() {
    return connectors.keySet();
  }

  ImmutableCollection<Connector> getAllConnectors() {
    return connectors.values();
  }

  @Nullable
  public Connector getByName(@Nonnull String connectorName) {
    return connectors.get(connectorName.toLowerCase());
  }
}
