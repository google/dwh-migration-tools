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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Validate if the provided tables exist/are accessible for the task and can be used later in the
 * tasks flow
 */
public class TeradataTablesValidatorTask extends AbstractJdbcTask<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(TeradataTablesValidatorTask.class);

  private final ImmutableList<String> tableNames;

  public TeradataTablesValidatorTask(@Nonnull String... tableNames) {
    super(TeradataTablesValidatorTask.class.getSimpleName() + ".txt", false);
    Preconditions.checkNotNull(tableNames, "Validated table names are null");
    Preconditions.checkArgument(tableNames.length > 0, "Validated table names are empty");

    this.tableNames = ImmutableList.copyOf(tableNames);
  }

  @CheckForNull
  @Override
  protected Void doInConnection(
      @Nonnull TaskRunContext context,
      @Nonnull JdbcHandle jdbcHandle,
      @Nonnull ByteSink sink,
      @Nonnull Connection connection)
      throws SQLException {
    LOG.debug("Checking accessible for the tables {}", tableNames);

    JdbcTemplate jdbcTemplate = jdbcHandle.getJdbcTemplate();
    List<String> notAccessibleTables = new ArrayList<>();

    for (String table : tableNames) {
      try {
        jdbcTemplate.queryForRowSet("select top 1 1 from " + table);
        LOG.trace("The table {} is accessible.", table);
      } catch (DataAccessException e) {
        LOG.error("The table {} is not accessible.", table, e);
        notAccessibleTables.add(table);
      }
    }
    if (!notAccessibleTables.isEmpty()) {
      throw new MetadataDumperUsageException(
          "The tables " + notAccessibleTables + " do not exists or are not accessible.");
    }

    LOG.debug("Success. The tables are accessible.");
    return null;
  }
}
