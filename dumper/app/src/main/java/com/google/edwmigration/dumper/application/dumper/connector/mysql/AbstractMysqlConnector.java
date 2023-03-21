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
package com.google.edwmigration.dumper.application.dumper.connector.mysql;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriverRequired;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentHostUnlessUrl;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPassword;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUri;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUser;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractJdbcConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import java.sql.Driver;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

/** @author shevek */
@RespectsArgumentDriverRequired
@RespectsArgumentHostUnlessUrl
@RespectsInput(
    order = ConnectorArguments.OPT_PORT_ORDER,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the server.",
    required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
    defaultValue = "" + AbstractMysqlConnector.OPT_PORT_DEFAULT)
@RespectsArgumentUser
@RespectsArgumentPassword
@RespectsArgumentUri
public abstract class AbstractMysqlConnector extends AbstractJdbcConnector {

  public static final int OPT_PORT_DEFAULT = 3306;

  public AbstractMysqlConnector(String name) {
    super(name);
  }

  @Override
  public Handle open(ConnectorArguments arguments) throws Exception {
    String url = arguments.getUri();
    if (url == null) {
      StringBuilder buf = new StringBuilder("jdbc:mysql://");
      buf.append(arguments.getHost("localhost"));
      buf.append(':').append(arguments.getPort(OPT_PORT_DEFAULT));
      buf.append("/");
      List<String> databases = arguments.getDatabases();
      if (!databases.isEmpty()) buf.append(databases.get(0));
      url = buf.toString();
    }

    Driver driver =
        newDriver(arguments.getDriverPaths(), "com.mysql.jdbc.Driver", "org.mariadb.jdbc.Driver");
    DataSource dataSource =
        new SimpleDriverDataSource(driver, url, arguments.getUser(), arguments.getPassword());
    return new JdbcHandle(dataSource);
  }
}
