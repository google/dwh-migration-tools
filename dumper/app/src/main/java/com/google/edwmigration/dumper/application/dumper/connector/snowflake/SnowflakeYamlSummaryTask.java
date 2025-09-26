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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import org.springframework.jdbc.core.ResultSetExtractor;

/** A {@link Task} that creates YAML with extraction metadata. */
@ParametersAreNonnullByDefault
final class SnowflakeYamlSummaryTask extends AbstractJdbcTask<Void> {

  private final boolean isAssessment;

  @Override
  public final String describeSourceData() {
    return "containing dump metadata.";
  }

  static ImmutableMap<String, String> createRoot(boolean isAssessment, String count)
      throws IOException {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("assessment", String.valueOf(isAssessment));
    builder.put("warehouse_count", count);
    return builder.build();
  }

  static String rootString(boolean isAssessment, String count) {
    try {
      ImmutableMap<String, String> root = createRoot(isAssessment, count);
      return CoreMetadataDumpFormat.MAPPER.writeValueAsString(root);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nonnull
  static SnowflakeYamlSummaryTask create(String zipFormat, ConnectorArguments arguments) {
    return new SnowflakeYamlSummaryTask(arguments.isAssessment());
  }

  private SnowflakeYamlSummaryTask(boolean isAssessment) {
    super("snowflake.yaml");
    this.isAssessment = isAssessment;
  }

  @Override
  protected Void doInConnection(
      TaskRunContext context, JdbcHandle jdbcHandle, ByteSink sink, Connection connection)
      throws SQLException {
    ResultSetExtractor<Void> extractor =
        resultSet -> {
          try {
            Object value = String.valueOf(resultSet.getMetaData().getColumnCount());
            action(sink, isAssessment, String.valueOf(value));
            return null;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
    String sql = "SHOW WAREHOUSES ->> SELECT count(*) FROM $1";
    doSelect(connection, extractor, sql);
    return null;
  }

  private static void action(ByteSink sink, boolean isAssessment, String count) throws IOException {

    CharSink streamSupplier = sink.asCharSink(UTF_8);
    try (Writer writer = streamSupplier.openBufferedStream()) {
      String value = rootString(isAssessment, count);
      CoreMetadataDumpFormat.MAPPER.writeValue(writer, value);
    }
  }
}
