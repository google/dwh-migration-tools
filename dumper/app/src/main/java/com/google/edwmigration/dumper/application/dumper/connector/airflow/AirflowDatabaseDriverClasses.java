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
package com.google.edwmigration.dumper.application.dumper.connector.airflow;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public enum AirflowDatabaseDriverClasses {
  MYSQL_OLD("com.mysql.jdbc.Driver", "jdbc:mysql://"),
  MYSQL("com.mysql.cj.jdbc.Driver", "jdbc:mysql://"),
  MARIADB("org.mariadb.jdbc.Driver", "jdbc:mariadb://"),
  POSTGRESQL("org.postgresql.Driver", "jdbc:postgresql://");
  private final String driverClassName;
  private final String jdbcStringPrefix;
  static final ImmutableMap<String, AirflowDatabaseDriverClasses> classNameToEnum;

  static {
    ImmutableMap.Builder<String, AirflowDatabaseDriverClasses> builder =
        new ImmutableMap.Builder<>();
    for (AirflowDatabaseDriverClasses driverClassName : AirflowDatabaseDriverClasses.values()) {
      builder.put(driverClassName.getDriverClassName(), driverClassName);
    }
    classNameToEnum = builder.build();
  }

  AirflowDatabaseDriverClasses(String driverClassName, String jdbcStringPrefix) {
    this.driverClassName = driverClassName;
    this.jdbcStringPrefix = jdbcStringPrefix;
  }

  public String getDriverClassName() {
    return driverClassName;
  }

  public String getJdbcStringPrefix() {
    return jdbcStringPrefix;
  }

  public static String jdbcPrefixForClassName(String driverClassName) {
    AirflowDatabaseDriverClasses driverClass = classNameToEnum.get(driverClassName);
    Preconditions.checkNotNull(driverClass, "Unsupported driver class name: " + driverClassName);
    return driverClass.jdbcStringPrefix;
  }
}
