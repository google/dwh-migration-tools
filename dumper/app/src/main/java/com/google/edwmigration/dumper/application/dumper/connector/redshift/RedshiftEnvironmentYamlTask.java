/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftMetadataDumpFormat.RedshiftEnvironmentFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftMetadataDumpFormat.RedshiftEnvironmentFormat.Root;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.springframework.jdbc.core.ResultSetExtractor;

public class RedshiftEnvironmentYamlTask extends AbstractJdbcTask<Void> {

  private static final String ENV_QUERY =
      "SELECT current_database() AS db_name, version() as db_version;";

  public RedshiftEnvironmentYamlTask() {
    super(RedshiftEnvironmentFormat.ZIP_ENTRY_NAME);
  }

  @CheckForNull
  @Override
  protected Void doInConnection(
      @Nonnull TaskRunContext context,
      @Nonnull JdbcHandle jdbcHandle,
      @Nonnull ByteSink sink,
      @Nonnull Connection connection)
      throws SQLException {
    ResultSetExtractor<Void> rse = getRedshiftEnvironmentExtractor(sink);
    return doSelect(connection, rse, ENV_QUERY);
  }

  private ResultSetExtractor<Void> getRedshiftEnvironmentExtractor(@Nonnull ByteSink sink) {
    return rs -> {
      try (RecordProgressMonitor monitor = new RecordProgressMonitor(getName());
          Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
        if (rs.next()) {
          monitor.count();
          Root root = new Root();
          root.currentDatabase = rs.getString("db_name");
          root.redshiftVersion = rs.getString("db_version");
          CoreMetadataDumpFormat.MAPPER.writeValue(writer, root);
        }
        return null;
      } catch (IOException e) {
        throw new SQLException(e);
      }
    };
  }
}
