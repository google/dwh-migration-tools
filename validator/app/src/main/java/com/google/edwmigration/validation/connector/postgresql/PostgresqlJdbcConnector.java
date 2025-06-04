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
package com.google.edwmigration.validation.connector.postgresql;

import com.google.edwmigration.validation.connector.api.Handle;
import com.google.edwmigration.validation.connector.common.AbstractJdbcConnector;
import com.google.edwmigration.validation.connector.jdbc.JdbcHandle;
import com.google.edwmigration.validation.model.ExecutionState;
import java.sql.Driver;
import java.util.List;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

public abstract class PostgresqlJdbcConnector extends AbstractJdbcConnector {
  public PostgresqlJdbcConnector(@Nonnull String name) {
    super(name);
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ExecutionState state) throws Exception {
    String url = state.context.sourceConnection.uri;
    if (url == null) {
      String database = state.context.sourceConnection.database;
      if (database != null) {
        url =
            "jdbc:postgresql://"
                + state.context.sourceConnection.host
                + ":"
                + state.context.sourceConnection.port
                + "/";
        url = url + database;
      }
    }

    String driverPaths = state.context.sourceConnection.driver;
    if (driverPaths == null) {
      throw new IllegalArgumentException("No PostgreSQL driver path provided.");
    } else {
      Driver driver =
          newDriver(List.of(state.context.sourceConnection.driver), "org.postgresql.Driver");
      DataSource dataSource = newSimpleDataSource(driver, url, state.context.sourceConnection);
      return new JdbcHandle(dataSource);
    }
  }
}
