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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorPropertyWithDefault;
import java.io.IOException;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectorProperties {
  private static final Logger logger = LoggerFactory.getLogger(ConnectorProperties.class);

  private final ImmutableMap<String, String> definitionMap;

  ConnectorProperties(String connectorName, List<String> definitions) {
    definitionMap = buildDefinitionMap(connectorName, definitions);
  }

  ImmutableMap<String, String> getDefinitionMap() {
    return definitionMap;
  }

  @CheckForNull
  String get(@Nonnull ConnectorProperty property) {
    return definitionMap.get(property.getName());
  }

  /** Checks if the property was specified on the command-line. */
  boolean isSpecified(@Nonnull ConnectorProperty property) {
    return StringUtils.isNotEmpty(definitionMap.get(property.getName()));
  }

  @CheckForNull
  String getOrDefault(ConnectorPropertyWithDefault property) {
    String stringValue = get(property);
    if (StringUtils.isEmpty(stringValue)) {
      return property.getDefaultValue();
    }
    return stringValue;
  }

  static void printHelp(@Nonnull Appendable out, @Nonnull Connector connector) throws IOException {
    for (ConnectorProperty property : connector.getPropertyConstants()) {
      out.append("        ")
          .append("-D")
          .append(property.getName())
          .append("=value")
          .append("\t\t")
          .append(property.getDescription())
          .append("\n");
    }
  }

  private static ImmutableMap<String, String> buildDefinitionMap(
      @Nullable String connector, List<String> definitions) {
    ImmutableMap.Builder<String, String> resultMap = ImmutableMap.builder();
    ImmutableSetMultimap<String, String> propertyNamesByConnector = allPropertyNamesByConnector();
    ImmutableSet<String> allPropertyNames = ImmutableSet.copyOf(propertyNamesByConnector.values());
    for (String definition : definitions) {
      if (definition.contains("=")) {
        int idx = definition.indexOf("=");
        String name = definition.substring(0, idx);
        String value = definition.substring(idx + 1);
        resultMap.put(name, value);
        if (connector != null && propertyNamesByConnector.get(connector).contains(name)) {
          logger.info("Parsed property: name='{}', value='{}'", name, value);
        } else if (allPropertyNames.contains(name)) {
          throw new MetadataDumperUsageException(
              String.format(
                  "Property: name='%s', value='%s' is not compatible with connector '%s'",
                  name, value, MoreObjects.firstNonNull(connector, "[not specified]")));
        } else {
          throw new MetadataDumperUsageException(
              String.format("Unknown property: name='%s', value='%s'", name, value));
        }
      }
    }
    return resultMap.buildKeepingLast();
  }

  private static ImmutableSetMultimap<String, String> allPropertyNamesByConnector() {
    ImmutableSetMultimap.Builder<String, String> connectorPropertyNames =
        ImmutableSetMultimap.builder();
    for (Connector connector : ConnectorRepository.getInstance().getAllConnectors()) {
      String connectorName = connector.getName();
      for (ConnectorProperty property : connector.getPropertyConstants()) {
        connectorPropertyNames.put(connectorName, property.getName());
      }
    }
    return connectorPropertyNames.build();
  }
}
