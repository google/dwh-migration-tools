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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriverClass;
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
import javax.annotation.Nonnull;
import javax.sql.DataSource;

/**
 * Note that connecting to any Teradata database requires their JDBC driver (not included here for
 * obvious reasons). For details on how to obtain and use this driver, see the asciidoc
 * documentation entitled "Metadata and Query Log Dumper"
 *
 * <p>Teradata table references:
 * http://elsasoft.com/samples/teradata/Teradata.127.0.0.1.DBC/allTables.htm
 * https://docs.teradata.com/reader/B7Lgdw6r3719WUyiCSJcgw/zpnjJYg3OPoAeMcGbnKuNQ
 *
 * @author matt
 */
@RespectsArgumentHostUnlessUrl
@RespectsInput(
    order = ConnectorArguments.OPT_PORT_ORDER,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the server.",
    required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
    defaultValue = "" + AbstractTeradataConnector.OPT_PORT_DEFAULT)
@RespectsArgumentUser
@RespectsArgumentPassword
@RespectsArgumentDriverRequired
@RespectsArgumentUri
@RespectsArgumentDriverClass
public abstract class AbstractTeradataConnector extends AbstractJdbcConnector {

  public static final int OPT_PORT_DEFAULT = 1025;

  /* pp */ AbstractTeradataConnector(@Nonnull String name) {
    super(name);
  }

  @Nonnull
  @Override
  public Handle open(ConnectorArguments arguments) throws Exception {
    String url = arguments.getUri();
    if (url == null) {
      String host = arguments.getHost();
      int port = arguments.getPort(OPT_PORT_DEFAULT);
      url = "jdbc:teradata://" + host + "/DBS_PORT=" + port + ",TMODE=ANSI,CHARSET=UTF8";
      // ,MAX_MESSAGE_BODY=16777216
    }

    Driver driver =
        newDriver(
            arguments.getDriverPaths(), arguments.getDriverClass("com.teradata.jdbc.TeraDriver"));
    DataSource dataSource = newSimpleDataSource(driver, url, arguments);
    // Teradata has a hard connection limit; let's stay below it, and block threads if required.
    // This could probably be 1 because the Teradata dumper is single-threaded.
    return JdbcHandle.newPooledJdbcHandle(dataSource, 2);
  }
}
