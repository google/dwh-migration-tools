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
package com.google.edwmigration.dumper.application.dumper.test;

import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import javax.annotation.Nonnull;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

/** @author shevek */
public class DumperTestUtils {

  private static final String JDBC_URL_PREFIX = "jdbc:sqlite:";

  @Nonnull
  public static File newZipFile(@Nonnull String name) {
    return TestUtils.newOutputFileUnchecked("dumper/" + name + ".zip");
  }

  @Nonnull
  public static File newJdbcFile(@Nonnull String name) {
    return TestUtils.newOutputFileUnchecked("dumper/" + name + ".db");
  }

  @Nonnull
  public static ZonedDateTime getTimeSubtractingDays(int days) {
    ZonedDateTime nowAtUTC = ZonedDateTime.now(ZoneOffset.UTC);
    return nowAtUTC.minusDays(days).truncatedTo(ChronoUnit.HOURS);
  }

  @Nonnull
  public static JdbcHandle newJdbcHandle(@Nonnull File file) throws IOException, SQLException {
    SQLiteDataSource dataSource = new SQLiteDataSource(new SQLiteConfig());
    dataSource.setUrl(JDBC_URL_PREFIX + file.getAbsolutePath());
    final Connection connection = dataSource.getConnection();
    // Now, we muck around to make sure we only use a single persistent connection
    // so that using the JdbcTemplate to create tables or attach things
    // leaves them attached for the later consumers.
    return new JdbcHandle(new SingleConnectionDataSource(connection, true)) {
      @Override
      public void close() throws IOException {
        try {
          connection.close();
        } catch (SQLException e) {
          throw new IOException(e);
        } finally {
          super.close();
        }
      }
    };
  }
}
