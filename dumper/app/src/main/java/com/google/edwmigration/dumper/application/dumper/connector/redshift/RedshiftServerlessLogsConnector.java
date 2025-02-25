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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import static com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil.getEntryFileNameWithTimestamp;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;

import java.time.ZonedDateTime;
import java.util.List;
import javax.annotation.Nonnull;

@AutoService({Connector.class})
@Description("Dumps logs from Amazon Redshift Serverless.")
@RespectsInput(
    order = ConnectorArguments.OPT_PORT_ORDER,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the server.",
    required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
    defaultValue = RedshiftUrlUtil.OPT_PORT_DEFAULT)
@RespectsArgumentAssessment
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
public class RedshiftServerlessLogsConnector extends AbstractRedshiftConnector
    implements LogsConnector {

  private static final String ZIP_SERVERLESS_USAGE_ENTRY_PREFIX = "sys_serverless_usage_";
  private static final String FORMAT_NAME = "redshift-serverless-logs.zip";

  public RedshiftServerlessLogsConnector() {
    super("redshift-serverless-logs");
  }

  @Override
  public void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    // Redshift retains serverless usage data for max 7 days. This results in relatively low
    // number of records so there is no need to split it into the smaller, interval based files.
    String file_name = getEntryFileNameWithTimestamp(ZIP_SERVERLESS_USAGE_ENTRY_PREFIX, ZonedDateTime.now());
    out.add(
        new JdbcSelectTask(file_name, "SELECT * FROM SYS_SERVERLESS_USAGE"));
  }
}
