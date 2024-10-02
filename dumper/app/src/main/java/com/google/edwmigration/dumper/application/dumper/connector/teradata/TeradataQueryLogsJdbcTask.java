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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ResultSetExtractor;

public class TeradataQueryLogsJdbcTask extends JdbcSelectTask {

  private static final Logger LOG = LoggerFactory.getLogger(TeradataQueryLogsJdbcTask.class);

  public TeradataQueryLogsJdbcTask(String targetPath, String sql) {
    super(targetPath, sql);
  }

  @Override
  protected Summary doInConnection(
      @Nonnull TaskRunContext context,
      @Nonnull JdbcHandle jdbcHandle,
      @Nonnull ByteSink sink,
      @Nonnull Connection connection)
      throws SQLException {

    LOG.info("Starting getting first and last entry of query logs from");
    ResultSetExtractor<Summary> rse = newCsvResultSetExtractor(sink);
    Summary summary = doSelect(connection, rse, getSql());
    LOG.info("Result of getting first and last entry is '%s", summary);
    return null;
  }

  // protected ResultSetExtractor<TeradataQueryLogResults> newResultSetExtractor(
  //     @Nonnull ByteSink sink) {
  //   return new ResultSetExtractor<TeradataQueryLogResults>() {
  //     @Override
  //     public TeradataQueryLogResults extractData(ResultSet rs)
  //         throws SQLException, DataAccessException {
  //       TeradataQueryLogResults out = new TeradataQueryLogResults();
  //       try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {

  //         return out;
  //       } catch (IOException e) {
  //         throw new SQLException(e);
  //       }
  //     }
  //   };
  // }
}
