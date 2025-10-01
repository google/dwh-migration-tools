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

import static com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.LITERAL_BLOCK_STYLE;
import static com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.SPLIT_LINES;
import static com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.WRITE_DOC_START_MARKER;
import static java.lang.Integer.parseInt;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

/** A {@link Task} that creates YAML with extraction metadata. */
@ParametersAreNonnullByDefault
final class SnowflakeYamlSummaryTask extends AbstractJdbcTask<Void> {

  private static final YAMLFactory yamlFactory =
      new YAMLFactory()
          .enable(LITERAL_BLOCK_STYLE)
          .disable(WRITE_DOC_START_MARKER)
          .disable(SPLIT_LINES);
  private static final ObjectMapper mapper =
      new ObjectMapper(yamlFactory).enable(SerializationFeature.INDENT_OUTPUT);

  private final boolean isAssessment;

  @Override
  public final String describeSourceData() {
    return "containing dump metadata.";
  }

  static ImmutableMap<String, ?> createRoot(boolean isAssessment, Optional<String> optionalCount) {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put("assessment", isAssessment);
    String count = optionalCount.orElse(null);
    int parsed;
    if (count == null) {
      parsed = 0;
    } else {
      parsed = parseInt(count);
    }
    builder.put("warehouse_count", parsed);
    return ImmutableMap.of("metadata", builder.build());
  }

  static String rootString(boolean isAssessment, Optional<String> optionalCount) {
    try {
      ImmutableMap<String, ?> root = createRoot(isAssessment, optionalCount);
      return mapper.writeValueAsString(root);
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
    String sql = "SHOW WAREHOUSES ->> SELECT count(*) FROM $1";
    return doSelect(connection, new Extractor(sink, isAssessment), sql);
  }

  static final class Extractor implements ResultSetExtractor<Void> {
    private final ByteSink sink;
    private final boolean isAssessment;

    Extractor(ByteSink sink, boolean isAssessment) {
      this.sink = sink;
      this.isAssessment = isAssessment;
    }

    @Override
    public Void extractData(@Nullable ResultSet rs) throws SQLException, DataAccessException {
      doExtract(requireNonNull(rs));
      return null;
    }

    void doExtract(ResultSet resultSet) throws SQLException, DataAccessException {
      resultSet.next();
      Optional<String> count = Optional.ofNullable(resultSet.getString(1));
      String yaml = rootString(isAssessment, count);
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream()) {
        mapper.writeValue(writer, yaml);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
