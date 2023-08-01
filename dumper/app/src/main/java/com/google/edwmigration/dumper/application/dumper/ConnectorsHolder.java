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
package com.google.edwmigration.dumper.application.dumper;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import java.util.Optional;
import java.util.ServiceLoader;

class ConnectorsHolder {

  private static final ImmutableMap<String, Connector> CONNECTORS;

  static {
    ImmutableMap.Builder<String, Connector> builder = ImmutableMap.builder();
    for (Connector connector : ServiceLoader.load(Connector.class)) {
      builder.put(connector.getName().toLowerCase(), connector);
    }
    CONNECTORS = builder.build();
  }

  static Optional<Connector> getConnector(String name) {
    return Optional.ofNullable(CONNECTORS.get(name.toLowerCase()));
  }

  static ImmutableSet<String> getConnectorNames() {
    return CONNECTORS.keySet();
  }

  static ImmutableCollection<Connector> getAllConnectors() {
    return CONNECTORS.values();
  }
}
