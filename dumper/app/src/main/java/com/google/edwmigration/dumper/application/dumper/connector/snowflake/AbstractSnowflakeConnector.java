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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import com.google.common.base.Joiner;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabaseForConnection;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriver;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentHostUnlessUrl;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPassword;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUri;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUser;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInputs;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractJdbcConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

/** @author shevek */
@RespectsArgumentHostUnlessUrl
@RespectsArgumentUser
@RespectsArgumentPassword
@RespectsInputs({
  // Although RespectsInput is @Repeatable, errorprone fails on it.
  @RespectsInput(
      order = 450,
      arg = ConnectorArguments.OPT_ROLE,
      description = "The Snowflake role to use for authorization."),
  @RespectsInput(
      order = 500,
      arg = ConnectorArguments.OPT_WAREHOUSE,
      description = "The Snowflake warehouse to use for processing metadata queries.")
})
@RespectsArgumentDatabaseForConnection
@RespectsArgumentDriver
@RespectsArgumentUri
public abstract class AbstractSnowflakeConnector extends AbstractJdbcConnector {

  public AbstractSnowflakeConnector(@Nonnull String name) {
    super(name);
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    String url = arguments.getUri();
    if (url == null) {
      StringBuilder buf = new StringBuilder("jdbc:snowflake://");
      String host = arguments.getHost("host.snowflakecomputing.com");
      buf.append(host).append("/");
      // FWIW we can/should totally use a Properties object here and pass it to
      // SimpleDriverDataSource rather than messing with the URL.
      List<String> optionalArguments = new ArrayList<>();
      if (arguments.getWarehouse() != null)
        optionalArguments.add("warehouse=" + arguments.getWarehouse());
      if (!arguments.getDatabases().isEmpty())
        optionalArguments.add("db=" + arguments.getDatabases().get(0));
      if (arguments.getRole() != null) optionalArguments.add("role=" + arguments.getRole());
      if (!optionalArguments.isEmpty())
        buf.append("?").append(Joiner.on("&").join(optionalArguments));
      url = buf.toString();
    }

    Driver driver =
        newDriver(arguments.getDriverPaths(), "net.snowflake.client.jdbc.SnowflakeDriver");
    DataSource dataSource =
        new SimpleDriverDataSource(driver, url, arguments.getUser(), arguments.getPassword());
    return checkCurrentDatabaseExists(arguments, new JdbcHandle(dataSource));
  }

  @Nonnull
  private JdbcHandle checkCurrentDatabaseExists(
      @Nonnull ConnectorArguments arguments, @Nonnull JdbcHandle jdbcHandle)
      throws MetadataDumperUsageException {
    JdbcTemplate jdbcTemplate = jdbcHandle.getJdbcTemplate();
    String currentDatabase = jdbcTemplate.queryForObject("SELECT CURRENT_DATABASE()", String.class);
    if (currentDatabase == null) {
      List<String> dbNames =
          jdbcTemplate.query("SHOW DATABASES", (rs, rowNum) -> rs.getString("name"));
      throw new MetadataDumperUsageException(
          "Database name not found "
              + arguments.getDatabases().get(0)
              + ", use one of: "
              + StringUtils.join(dbNames, ", "));
    }
    return jdbcHandle;
  }
}
