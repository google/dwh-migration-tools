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
package com.google.edwmigration.validation;

import static com.google.common.base.MoreObjects.firstNonNull;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.edwmigration.validation.io.PasswordReader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class ValidationConnection {

  private static final Logger LOG = LoggerFactory.getLogger(ValidationConnection.class);
  private final String connectionType;
  private final String driver;
  private final String uri;
  private final String jdbcDriverClass;
  private final String host;
  private final String port;
  private final String user;
  private final PasswordReader passwordReader = new PasswordReader();
  private final String password;
  private final String database;
  private final String projectId;
  private final String serviceAccount;

  public ValidationConnection(
      String connectionType,
      String driver,
      String host,
      String port,
      String user,
      String password,
      String database,
      String projectId,
      String serviceAccount,
      String uri,
      String jdbcDriverClass) {
    this.connectionType = connectionType;
    this.driver = driver;
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.database = database;
    this.projectId = projectId;
    this.serviceAccount = serviceAccount;
    this.uri = uri;
    this.jdbcDriverClass = jdbcDriverClass;
  }

  @CheckForNull
  public String getConnectionType() {
    return connectionType;
  }

  @CheckForNull
  public String getHost() {
    return host;
  }

  @Nonnull
  public String getHost(@Nonnull String defaultHost) {
    return firstNonNull(getHost(), defaultHost);
  }

  @CheckForNull
  public Integer getPort() {
    if (port != null) {
      return Integer.valueOf(port);
    }
    return null;
  }

  @Nonnegative
  public int getPort(@Nonnegative int defaultPort) {
    Integer customPort = getPort();
    if (customPort == null) {
      return defaultPort;
    }
    return customPort;
  }

  @CheckForNull
  public String getUser() {
    return user;
  }

  @CheckForNull
  public String getPassword() {
    if (password != null) {
      return password;
    } else {
      return passwordReader.getOrPrompt();
    }
  }

  @CheckForNull
  public String getServiceAccount() {
    return serviceAccount;
  }

  @CheckForNull
  public String getProjectId() {
    return projectId;
  }

  @CheckForNull
  public String getDatabase() {
    return database;
  }

  @CheckForNull
  public List<String> getDriverPaths() {
    try {
      return Arrays.stream(driver.split(","))
          .map(String::trim)
          .filter(StringUtils::isNotEmpty)
          .collect(Collectors.toList());
    } catch (NullPointerException e) {
      LOG.error("Null driver path provided.");
      return null;
    }
  }

  @CheckForNull
  public String getUri() {
    return uri;
  }

  @CheckForNull
  public String getDriverClass(String defaultDriverClass) {
    try {
      return firstNonNull(jdbcDriverClass, defaultDriverClass);
    } catch (NullPointerException e) {
      LOG.error("Null default DriverClass provided.");
      return null;
    }
  }

  @Override
  public String toString() {
    ToStringHelper toStringHelper =
        MoreObjects.toStringHelper(this)
            .add("connectionType", getConnectionType())
            .add("driver", getDriverPaths())
            .add("host", getHost())
            .add("port", getPort())
            .add("user", getUser())
            .add("database", getDatabase())
            .add("projectId", getProjectId())
            .add("serviceAccount", getServiceAccount())
            .omitNullValues();
    return toStringHelper.toString();
  }
}
