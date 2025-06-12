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
package com.google.edwmigration.dumper.application.dumper.metrics;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import java.time.LocalDateTime;
import java.util.*;

public class ArgumentSummary {
  private String connector;
  private boolean isAssessment;
  private String host;
  private Integer port;
  private String warehouse;
  private String user;
  private String output;
  private Integer queryLogDays;
  private LocalDateTime queryLogStart;
  private LocalDateTime getQueryLogEnd;
  private List<String> queryLogAlternates;
  private String driver;
  private List<String> database;
  private List<String> configuration;

  public ArgumentSummary(ConnectorArguments arguments) {
    this.connector = arguments.getConnectorName();
    this.host = arguments.getHost();
    this.port = arguments.getPort();
    this.warehouse = arguments.getWarehouse();
    this.user = arguments.getUser();
    this.output = arguments.getOutputFile().orElse(null);
    this.queryLogDays = arguments.getQueryLogDays();
    this.queryLogStart =
        arguments.getQueryLogStart() == null
            ? null
            : arguments.getQueryLogStart().toLocalDateTime();
    this.getQueryLogEnd =
        arguments.getQueryLogEnd() == null ? null : arguments.getQueryLogEnd().toLocalDateTime();
    this.queryLogAlternates = arguments.getQueryLogAlternates();
    this.driver = arguments.getDriverClass();
    this.database = arguments.getDatabases();
    this.configuration = arguments.getConfiguration();
    this.isAssessment = arguments.isAssessment();
  }

  public String getConnector() {
    return connector;
  }

  public void setConnector(String connector) {
    this.connector = connector;
  }

  public boolean isAssessment() {
    return isAssessment;
  }

  public void setAssessment(boolean assessment) {
    isAssessment = assessment;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  public String getWarehouse() {
    return warehouse;
  }

  public void setWarehouse(String warehouse) {
    this.warehouse = warehouse;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getOutput() {
    return output;
  }

  public void setOutput(String output) {
    this.output = output;
  }

  public Integer getQueryLogDays() {
    return queryLogDays;
  }

  public void setQueryLogDays(Integer queryLogDays) {
    this.queryLogDays = queryLogDays;
  }

  public LocalDateTime getQueryLogStart() {
    return queryLogStart;
  }

  public void setQueryLogStart(LocalDateTime queryLogStart) {
    this.queryLogStart = queryLogStart;
  }

  public LocalDateTime getGetQueryLogEnd() {
    return getQueryLogEnd;
  }

  public void setGetQueryLogEnd(LocalDateTime getQueryLogEnd) {
    this.getQueryLogEnd = getQueryLogEnd;
  }

  public List<String> getQueryLogAlternates() {
    return queryLogAlternates;
  }

  public void setQueryLogAlternates(List<String> queryLogAlternates) {
    this.queryLogAlternates = queryLogAlternates;
  }

  public String getDriver() {
    return driver;
  }

  public void setDriver(String driver) {
    this.driver = driver;
  }

  public List<String> getDatabase() {
    return database;
  }

  public void setDatabase(List<String> database) {
    this.database = database;
  }

  public List<String> getConfiguration() {
    return configuration;
  }

  public void setConfiguration(List<String> configuration) {
    this.configuration = configuration;
  }
}
