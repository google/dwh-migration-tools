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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.auto.value.AutoValue;
import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource;
import java.io.IOException;
import java.net.URL;
import javax.annotation.Nonnull;

@AutoValue
public abstract class OracleStatsQuery {

  abstract String name();

  abstract StatsSource tool();

  static OracleStatsQuery create(String name, StatsSource tool) {
    return new AutoValue_OracleStatsQuery(name, tool);
  }

  @Nonnull
  String queryText() throws IOException {
    String path = String.format("oracle-stats/%s/%s.sql", tool().value, name());
    URL queryUrl = Resources.getResource(path);
    return Resources.toString(queryUrl, UTF_8);
  }
}
