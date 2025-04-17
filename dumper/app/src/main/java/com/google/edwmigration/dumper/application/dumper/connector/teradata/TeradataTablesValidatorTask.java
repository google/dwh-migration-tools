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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validate if the provided tables exist/are accessible for the task and can be used later in the
 * tasks flow
 */
public class TeradataTablesValidatorTask extends AbstractJdbcTask<Void> {
  private static final Logger logger = LoggerFactory.getLogger(TeradataTablesValidatorTask.class);

  private final ImmutableSet<String> tableNames;

  public TeradataTablesValidatorTask(@Nonnull String... tableNames) {
    super(
        TeradataTablesValidatorTask.class.getSimpleName() + ".txt",
        TaskOptions.DEFAULT.withTargetInitialization(TargetInitialization.DO_NOT_CREATE));
    Preconditions.checkNotNull(tableNames, "Validated table names are null");
    Preconditions.checkArgument(tableNames.length > 0, "Validated table names are empty");

    this.tableNames = ImmutableSet.copyOf(tableNames);
  }

  @CheckForNull
  @Override
  protected Void doInConnection(
      @Nonnull TaskRunContext context,
      @Nonnull JdbcHandle jdbcHandle,
      @Nonnull ByteSink sink,
      @Nonnull Connection connection)
      throws SQLException {
    logger.debug("Checking availability for the tables {}", tableNames);

    List<String> notAccessibleTables = new ArrayList<>();

    for (String table : tableNames) {
      try {
        try (PreparedStatement statement = connection.prepareStatement("select 1 from " + table)) {
          // `select top 1  1 from X` doesn't work for SQLlite which is used in unit-tests
          // so the limitation is made with the statement/driver to support Teradata and SQLlite
          statement.setMaxRows(1);
          statement.execute();
        }
        logger.trace("The table {} is accessible.", table);
      } catch (SQLException e) {
        logger.error("The table {} is not accessible.", table, e);
        notAccessibleTables.add(table);
      }
    }
    if (!notAccessibleTables.isEmpty()) {
      throw new MetadataDumperUsageException(
          "The tables " + notAccessibleTables + " do not exists or are not accessible.");
    }

    logger.debug("Success. The tables are accessible.");
    return null;
  }

  @VisibleForTesting
  @Nonnull
  ImmutableSet<String> getTableNames() {
    return tableNames;
  }
}
