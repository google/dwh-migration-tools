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

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Represents a strategy of getting Snowflake data.
 *
 * <p>A strategy for getting data from Snowflake. USAGE stands for ACCOUNT_USAGE, SCHEMA is
 * INFORMATION_SCHEMA.
 *
 * <p>Docref: https://docs.snowflake.net/manuals/sql-reference/info-schema.html#list-of-views
 * ACCOUNT_USAGE is much faster than INFORMATION_SCHEMA and does not have the size limitations, but
 * requires extra privileges to be granted.
 * https://docs.snowflake.net/manuals/sql-reference/account-usage.html
 * https://docs.snowflake.net/manuals/user-guide/data-share-consumers.html You must: GRANT IMPORTED
 * PRIVILEGES ON DATABASE snowflake TO ROLE <SOMETHING>;
 */
@ParametersAreNonnullByDefault
enum SnowflakeInput {
  /** Get data from ACCOUNT_USAGE contents, with a fallback to INFORMATION_SCHEMA. */
  USAGE_THEN_SCHEMA_SOURCE {
    @Override
    @Nonnull
    ImmutableList<Task<?>> sqlTasks(AbstractJdbcTask<?> schemaTask, AbstractJdbcTask<?> usageTask) {
      return ImmutableList.of(usageTask, schemaTask.onlyIfFailed(usageTask));
    }
  },
  /** Get data relying only on the contents of INFORMATION_SCHEMA */
  SCHEMA_ONLY_SOURCE {
    @Override
    @Nonnull
    ImmutableList<Task<?>> sqlTasks(AbstractJdbcTask<?> schemaTask, AbstractJdbcTask<?> usageTask) {
      return ImmutableList.of(schemaTask);
    }
  },
  /** Get data relying only on the contents of ACCOUNT_USAGE */
  USAGE_ONLY_SOURCE {
    @Override
    @Nonnull
    ImmutableList<Task<?>> sqlTasks(AbstractJdbcTask<?> schemaTask, AbstractJdbcTask<?> usageTask) {
      return ImmutableList.of(usageTask);
    }
  };

  @Nonnull
  abstract ImmutableList<Task<?>> sqlTasks(AbstractJdbcTask<?> schema, AbstractJdbcTask<?> usage);
}
