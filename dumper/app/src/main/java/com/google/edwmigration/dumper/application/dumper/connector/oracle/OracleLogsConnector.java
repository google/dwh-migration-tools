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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.OracleLogsDumpFormat;
import java.util.List;
import javax.annotation.Nonnull;

@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps query logs from Oracle")
public class OracleLogsConnector extends AbstractOracleConnector
    implements LogsConnector, OracleLogsDumpFormat {

  public OracleLogsConnector() {
    super("oracle-logs");
  }

  /** Exists so we can extract query text CLOBs to Strings before they reach the CSVPrinter. */
  private static class QueryHistoryTask extends JdbcSelectTask {

    public QueryHistoryTask(@Nonnull String targetPath, @Nonnull String sql) {
      super(targetPath, sql);
    }
  }

  @Override
  public void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    // It's not clear to me whether we should be using v$sqlarea instead here.
    String query =
        "SELECT sql_fulltext, cpu_time, elapsed_time, disk_reads, runtime_mem FROM v$sql";
    out.add(new QueryHistoryTask(ZIP_ENTRY_FILENAME, query).withHeaderClass(Header.class));
  }
}
