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
package com.google.edwmigration.dumper.application.dumper.connector.postgresql;

import com.google.common.collect.Iterables;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriver;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriverClass;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentHostUnlessUrl;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPassword;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUri;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUser;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractJdbcConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import java.sql.Driver;
import javax.sql.DataSource;

/** @author shevek */
@RespectsArgumentDriver
@RespectsArgumentDriverClass
@RespectsArgumentHostUnlessUrl
@RespectsArgumentUser
@RespectsArgumentPassword
@RespectsArgumentUri
public abstract class AbstractPostgresqlConnector extends AbstractJdbcConnector {

  public static final int OPT_PORT_DEFAULT = 5432;

  public AbstractPostgresqlConnector(String name) {
    super(name);
  }

  @Override
  public Handle open(ConnectorArguments arguments) throws Exception {
    String url = arguments.getUri();
    if (url == null) {
      String host = arguments.getHost("localhost");
      int port = arguments.getPort(OPT_PORT_DEFAULT);
      String database = Iterables.getFirst(arguments.getDatabases(), null);
      url = "jdbc:postgresql://" + host + ":" + port + "/";
      if (database != null) url = url + database;
    }

    Driver driver =
        newDriver(arguments.getDriverPaths(), arguments.getDriverClass("org.postgresql.Driver"));
    DataSource dataSource = newSimpleDataSource(driver, url, arguments);
    return new JdbcHandle(dataSource);
  }
}
