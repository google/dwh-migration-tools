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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import java.sql.ResultSet;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import org.apache.commons.csv.CSVFormat;

@ParametersAreNonnullByDefault
final class LiteTimeSeriesTask extends JdbcSelectTask {
  private final ImmutableList<String> header;

  LiteTimeSeriesTask(String csvName, String sql, ImmutableList<String> header) {
    super(csvName, sql);
    this.header = header;
  }

  @Override
  @Nonnull
  protected CSVFormat newCsvFormat(ResultSet rs) {
    return FORMAT.builder().setHeader(header.toArray(new String[0])).build();
  }
}
