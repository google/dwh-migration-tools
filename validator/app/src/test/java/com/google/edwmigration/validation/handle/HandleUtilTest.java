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
package com.google.edwmigration.validation.handle;

import static com.google.edwmigration.validation.connector.jdbc.JdbcHandleUtil.createHikariConfig;
import static com.google.edwmigration.validation.connector.jdbc.JdbcHandleUtil.createJdbcTemplate;
// import static
// com.google.edwmigration.validation.connector.handle.HandleUtil.verifyJdbcConnection;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.zaxxer.hikari.HikariConfig;
import javax.sql.DataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.jdbc.core.JdbcTemplate;

@RunWith(MockitoJUnitRunner.class)
public class HandleUtilTest {

  @Test
  public void templateFromDataSource_success_fetchSizeMatches() {
    DataSource dataSource = mock(DataSource.class);

    JdbcTemplate template = createJdbcTemplate(dataSource);

    assertEquals(1024, template.getFetchSize());
  }

  //   @Test
  //   public void verifyJdbcConnection_nullConnection_exceptionThrown() throws SQLException {
  //     DataSource dataSource = mock(DataSource.class);
  //     when(dataSource.getConnection()).thenReturn(null);

  //     assertThrows(NullPointerException.class, () -> verifyJdbcConnection(dataSource));
  //   }

  @Test
  public void withPoolConfig_success_poolSizeMatches() {
    DataSource dataSource = mock(DataSource.class);

    HikariConfig config = createHikariConfig(dataSource, 2);

    assertEquals(2, config.getMaximumPoolSize());
  }

  @Test
  public void withPoolConfig_success_timeoutEqualsIntMax() {
    DataSource dataSource = mock(DataSource.class);

    HikariConfig config = createHikariConfig(dataSource, 1);

    assertEquals(Integer.MAX_VALUE, config.getConnectionTimeout());
  }
}
