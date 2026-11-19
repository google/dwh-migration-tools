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
package com.google.edwmigration.dumper.application.dumper.jdbc;

import com.google.common.base.Preconditions;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.sql.DataSource;

/** Minimal {@link DataSource} delegating directly to an explicit JDBC {@link Driver} instance. */
public class DriverDataSource implements DataSource {

  @Nullable private final Driver driver;
  @Nullable private final String url;
  @Nullable private final String username;
  @Nullable private final String password;
  @Nullable private final Properties connectionProperties;

  public DriverDataSource() {
    this.driver = null;
    this.url = null;
    this.username = null;
    this.password = null;
    this.connectionProperties = null;
  }

  public DriverDataSource(Driver driver, String url) {
    this(driver, url, null, null, null);
  }

  public DriverDataSource(
      Driver driver, String url, @Nullable String username, @Nullable String password) {
    this(driver, url, username, password, null);
  }

  public DriverDataSource(Driver driver, String url, @Nullable Properties conProps) {
    this(driver, url, null, null, conProps);
  }

  private DriverDataSource(
      Driver driver,
      String url,
      @Nullable String username,
      @Nullable String password,
      @Nullable Properties conProps) {
    this.driver = Preconditions.checkNotNull(driver, "Driver must not be null");
    Preconditions.checkNotNull(url, "URL must not be null");
    this.url = url.trim();
    this.username = username;
    this.password = password;
    if (conProps != null) {
      Properties copy = new Properties();
      copy.putAll(conProps);
      this.connectionProperties = copy;
    } else {
      this.connectionProperties = null;
    }
  }

  @Nullable
  public Driver getDriver() {
    return this.driver;
  }

  @Nullable
  public String getUrl() {
    return this.url;
  }

  @Nullable
  public String getUsername() {
    if (this.username != null) {
      return this.username;
    }
    return this.connectionProperties != null ? this.connectionProperties.getProperty("user") : null;
  }

  @Nullable
  public String getPassword() {
    if (this.password != null) {
      return this.password;
    }
    return this.connectionProperties != null
        ? this.connectionProperties.getProperty("password")
        : null;
  }

  @Nullable
  public Properties getConnectionProperties() {
    if (this.connectionProperties == null) {
      return null;
    }
    Properties copy = new Properties();
    copy.putAll(this.connectionProperties);
    return copy;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return getConnectionFromDriver(this.username, this.password);
  }

  @Override
  public Connection getConnection(@Nullable String username, @Nullable String password)
      throws SQLException {
    return getConnectionFromDriver(username, password);
  }

  protected Connection getConnectionFromDriver(@Nullable String username, @Nullable String password)
      throws SQLException {
    Properties mergedProps = new Properties();
    if (this.connectionProperties != null) {
      mergedProps.putAll(this.connectionProperties);
    }
    if (username != null) {
      mergedProps.setProperty("user", username);
    }
    if (password != null) {
      mergedProps.setProperty("password", password);
    }
    return getConnectionFromDriver(mergedProps);
  }

  protected Connection getConnectionFromDriver(Properties props) throws SQLException {
    Preconditions.checkState(this.driver != null, "Driver must not be null");
    Preconditions.checkState(this.url != null, "URL must not be null");
    return this.driver.connect(this.url, props);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return 0;
  }

  @Override
  public void setLoginTimeout(int timeout) throws SQLException {
    throw new UnsupportedOperationException("setLoginTimeout");
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    throw new UnsupportedOperationException("getLogWriter");
  }

  @Override
  public void setLogWriter(PrintWriter pw) throws SQLException {
    throw new UnsupportedOperationException("setLogWriter");
  }

  @Override
  public Logger getParentLogger() {
    return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw new SQLException(
        "DataSource of type ["
            + getClass().getName()
            + "] cannot be unwrapped as ["
            + iface.getName()
            + "]");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }
}
