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

import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author ishmum */
public class ConnectorPropertyValidator {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectorPropertyValidator.class);

  public static void validate(Connector connector, Map<String, String> definitionMap)
      throws IllegalArgumentException {
    Set<String> properties =
        Arrays.stream(connector.getConnectorProperties().getEnumConstants())
            .map(enumValue -> ((ConnectorProperty) enumValue).getName())
            .collect(Collectors.toSet());
    definitionMap
        .keySet()
        .forEach(
            key -> {
              if (!properties.contains(key)) {
                throw new IllegalArgumentException(
                    key + " is not a recognized option for " + connector.getName());
              }
            });
  }
}
