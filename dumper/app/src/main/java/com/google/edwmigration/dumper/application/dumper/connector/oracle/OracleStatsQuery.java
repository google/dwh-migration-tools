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

import static com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource.AWR;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource.NATIVE;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource.STATSPACK;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.auto.value.AutoValue;
import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@AutoValue
@ParametersAreNonnullByDefault
public abstract class OracleStatsQuery {

  abstract boolean isRequired();

  @Nonnull
  abstract Duration queriedDuration();

  @Nonnull
  abstract String name();

  @Nonnull
  abstract String queryText();

  @Nonnull
  abstract StatsSource statsSource();

  @Nonnull
  static OracleStatsQuery createAwr(String name, Duration queriedDuration) throws IOException {
    return create(false, queriedDuration, name, AWR);
  }

  @Nonnull
  static OracleStatsQuery createNative(String name, boolean isRequired, Duration queriedDuration)
      throws IOException {
    return create(isRequired, queriedDuration, name, NATIVE);
  }

  @Nonnull
  static OracleStatsQuery createStatspack(String name, Duration queriedDuration)
      throws IOException {
    return create(false, queriedDuration, name, STATSPACK);
  }

  private static OracleStatsQuery create(
      boolean isRequired, Duration queriedDuration, String name, StatsSource statsSource)
      throws IOException {
    String path = String.format("oracle-stats/cdb/%s/%s.sql", statsSource.value, name);
    return new AutoValue_OracleStatsQuery(
        isRequired, queriedDuration, name, loadFile(path), statsSource);
  }

  @Nonnull
  String description() {
    return String.format("Query{name=%s, statsSource=%s}", name(), statsSource());
  }

  private static String loadFile(String path) throws IOException {
    URL queryUrl = Resources.getResource(path);
    return Resources.toString(queryUrl, UTF_8);
  }
}
