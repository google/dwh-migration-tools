/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.validation.application.validator;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class ValidationConnection {

  private static final Logger LOG = LoggerFactory.getLogger(ValidationConnection.class);
  private final String connectionType;
  private final String driver;
  private final String host;
  private final String port;
  private final String user;
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
      String serviceAccount) {
    this.connectionType = connectionType;
    this.driver = driver;
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.database = database;
    this.projectId = projectId;
    this.serviceAccount = serviceAccount;
  }

  public String getConnectionType() {
    return connectionType;
  }

  public String getHost() {
    return host;
  }

  public String getPort() {
    return port;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public String getServiceAccount() {
    return serviceAccount;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getDatabase() {
    return database;
  }

  public String getDriver() {
    return driver;
  }

  @Override
  public String toString() {
    ToStringHelper toStringHelper =
        MoreObjects.toStringHelper(this)
            .add("connectionType", getConnectionType())
            .add("driver", getDriver())
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
