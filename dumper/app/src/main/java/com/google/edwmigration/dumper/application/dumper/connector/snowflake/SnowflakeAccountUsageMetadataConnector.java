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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeMetadataConnector.TaskVariant;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.util.List;

/** @author shevek */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from Snowflake, using ACCOUNT_USAGE only.")
public class SnowflakeAccountUsageMetadataConnector extends SnowflakeMetadataConnector {

  public SnowflakeAccountUsageMetadataConnector() {
    super("snowflake-account-usage-metadata");
  }

  @Override
  protected void addSqlTasks(
      List<? super Task<?>> out,
      Class<? extends Enum<?>> header,
      String format,
      TaskVariant is_task,
      TaskVariant au_task) {
    Task<?> t0 =
        new JdbcSelectTask(
                au_task.zipEntryName,
                String.format(format, au_task.schemaName, au_task.whereClause))
            .withHeaderClass(header);
    out.add(t0);
  }
}
