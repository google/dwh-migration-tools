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
package com.google.edwmigration.validation.connector.jdbc;

import com.zaxxer.hikari.HikariConfig;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

@ParametersAreNonnullByDefault
public class JdbcHandleUtil {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcHandleUtil.class);

  public static JdbcTemplate createJdbcTemplate(DataSource dataSource) {
    JdbcTemplate template = new JdbcTemplate(dataSource);
    template.setFetchSize(1024);
    return template;
  }

  public static HikariConfig createHikariConfig(DataSource dataSource, int threadPoolSize) {
    HikariConfig config = new HikariConfig();
    // providing "0" sets it to Integer.MAX_VALUE
    config.setConnectionTimeout(0);
    config.setDataSource(dataSource);
    // Question: If a connection goes out to lunch, causing getConnection() to block for a thread,
    // can that deadlock the dumper?
    config.setMaximumPoolSize(threadPoolSize);
    config.setMinimumIdle(0);
    return config;
  }

  private JdbcHandleUtil() {}
}
