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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import static com.google.edwmigration.dumper.application.dumper.connector.oracle.OracleStatsQuery.TenantSetup.MULTI_TENANT;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource.AWR;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource.NATIVE;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource.STATSPACK;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.auto.value.AutoValue;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@AutoValue
@ParametersAreNonnullByDefault
public abstract class OracleStatsQuery {

  @Nonnull
  abstract Duration queriedDuration();

  @Nonnull
  abstract QueryGroup queryGroup();

  @Nonnull
  abstract String name();

  @Nonnull
  abstract String queryText();

  @Nonnull
  static OracleStatsQuery createAwr(String name, Duration queriedDuration) {
    QueryGroup queryGroup = QueryGroup.create(false, AWR, MULTI_TENANT);
    return create(name, queryGroup, queriedDuration);
  }

  @Nonnull
  static OracleStatsQuery createNative(
      String name, boolean isRequired, Duration queriedDuration, TenantSetup tenantSetup) {
    QueryGroup queryGroup = QueryGroup.create(isRequired, NATIVE, tenantSetup);
    return create(name, queryGroup, queriedDuration);
  }

  @Nonnull
  static OracleStatsQuery createStatspack(String name, Duration queriedDuration) {
    QueryGroup queryGroup = QueryGroup.create(false, STATSPACK, MULTI_TENANT);
    return create(name, queryGroup, queriedDuration);
  }

  enum TenantSetup {
    MULTI_TENANT("cdb"),
    SINGLE_TENANT("dba");

    final String code;

    TenantSetup(String code) {
      this.code = code;
    }
  }

  static OracleStatsQuery create(String name, QueryGroup queryGroup, Duration queriedDuration) {
    String path = String.format("oracle-stats/%s/%s.sql", queryGroup.path(), name);
    return new AutoValue_OracleStatsQuery(queriedDuration, queryGroup, name, loadFile(path));
  }

  @Nonnull
  String description() {
    return String.format("Query{name=%s, statsSource=%s}", name(), queryGroup().statsSource());
  }

  private static String loadFile(String path) {
    try {
      URL queryUrl = Resources.getResource(path);
      return Resources.toString(queryUrl, UTF_8);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format("An invalid file was provided: '%s'.", path), e);
    }
  }
}
