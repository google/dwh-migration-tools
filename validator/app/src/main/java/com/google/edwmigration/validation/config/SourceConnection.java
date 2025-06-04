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
package com.google.edwmigration.validation.config;

import com.google.edwmigration.validation.connector.registry.ConnectorRegistry;
import com.google.edwmigration.validation.deformed.ValidationProperty;
import com.google.edwmigration.validation.deformed.ValidationSchema;
import java.util.List;
import java.util.Map;

public class SourceConnection {
  public String connectionType;
  public String database;
  public String driver;
  public String host;
  public String jdbcDriverClass;
  public String password;
  public String port;
  public String uri;
  public String user;

  public static final ValidationSchema<SourceConnection> buildSchema() {
    return new ValidationSchema<SourceConnection>(
        Map.of(
            "connectionType",
                List.of(
                    ValidationProperty.requiredString(
                        "connectionType is required", s -> s.connectionType),
                    ValidationProperty.of(
                        s ->
                            "connectionType "
                                + s.connectionType
                                + " is unsupported (available: "
                                + ConnectorRegistry.getInstance().getAllNames()
                                + ")",
                        s ->
                            s.connectionType != null
                                && s.connectionType.length() > 0
                                && ConnectorRegistry.getInstance().getByName(s.connectionType)
                                    != null)),
            "database",
                List.of(ValidationProperty.requiredString("database is required", s -> s.database)),
            "driver",
                List.of(ValidationProperty.requiredString("driver is required", s -> s.driver)),
            "host", List.of(ValidationProperty.requiredString("host is required", s -> s.host)),
            "password",
                List.of(ValidationProperty.requiredString("password is required", s -> s.password)),
            "port",
                List.of(
                    ValidationProperty.requiredString("port is required", s -> s.port),
                    ValidationProperty.of(
                        "port must be a number between 1 and 65535",
                        s -> {
                          try {
                            int port = Integer.parseInt(s.port.trim());
                            return port >= 1 && port <= 65535;
                          } catch (Exception e) {
                            return false;
                          }
                        })),
            "user", List.of(ValidationProperty.requiredString("user is required", s -> s.user))));
  }
}
