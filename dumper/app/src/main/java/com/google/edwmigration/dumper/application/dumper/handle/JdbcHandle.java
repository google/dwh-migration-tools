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
package com.google.edwmigration.dumper.application.dumper.handle;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.SQLException;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * This class is thread-safe as log as the provided {@link DataSource} is thread-safe.
 *
 * @author shevek
 */
public class JdbcHandle implements Handle {

  private static final Logger logger = LoggerFactory.getLogger(JdbcHandle.class);

  @Nonnull
  public static JdbcHandle newPooledJdbcHandle(@Nonnull DataSource dataSource, int threadPoolSize)
      throws SQLException {
    HikariConfig config = HandleUtil.createHikariConfig(dataSource, threadPoolSize);
    return new JdbcHandle(new HikariDataSource(config));
  }

  private final JdbcTemplate jdbcTemplate;

  public JdbcHandle(@Nonnull DataSource dataSource) throws SQLException {
    HandleUtil.verifyJdbcConnection(dataSource);
    this.jdbcTemplate = HandleUtil.createJdbcTemplate(dataSource);
  }

  @Nonnull
  public DataSource getDataSource() {
    return getJdbcTemplate().getDataSource();
  }

  @Nonnull
  public JdbcTemplate getJdbcTemplate() {
    return jdbcTemplate;
  }

  @Override
  public void close() throws IOException {
    DataSource ds = getDataSource();
    if (ds instanceof AutoCloseable) {
      try {
        ((AutoCloseable) ds).close();
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException("Failed to close DataSource: " + e, e);
      }
    }
  }
}
