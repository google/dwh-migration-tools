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
package com.google.edwmigration.validation.connector.jdbc;

import com.google.edwmigration.validation.model.ConnectionType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Handles connection with JDBC */
public class JdbcConnectionConfig {
  public ConnectionType connectionType;
  public String database;
  public String driver;
  public String user;
  public String password;
  public String host;
  public int port;

  public Connection connect() throws SQLException, ClassNotFoundException {
    // Register driver explicitly
    Class.forName("org.postgresql.Driver"); // TODO: dynamically load by connectionType

    String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    System.out.println("  → Connecting to: " + jdbcUrl);

    return DriverManager.getConnection(jdbcUrl, user, password);
  }
}
