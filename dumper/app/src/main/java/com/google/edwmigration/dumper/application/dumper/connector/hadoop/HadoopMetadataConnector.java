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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop;

import static com.google.edwmigration.dumper.plugin.lib.dumper.spi.HadoopMetadataDumpFormat.FORMAT_NAME;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.util.List;
import javax.annotation.Nonnull;

@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from the Hadoop cluster via bash commands.")
public class HadoopMetadataConnector implements MetadataConnector {

  @VisibleForTesting
  static final ImmutableList<String> SCRIPT_NAMES =
      ImmutableList.of(
          "airflow-version",
          "apt-list-packages",
          "docker-version",
          "gcc-version",
          "go-version",
          "gplusplus-version",
          "grafana-server-version",
          "hadoop-version",
          "hbase-shell-version",
          "hbase-version",
          "hive-version",
          "ip-address",
          "java-version",
          "ls-usr-local-lib",
          "ls-var-log",
          "lsb-release",
          "metastore-connection-details",
          "omd-version",
          "oozie-admin-status",
          "oozie-version",
          "os-release",
          "perl-version",
          "pig-version",
          "python-version",
          "r-version",
          "rpm-list-packages",
          "ruby-version",
          "scala-version",
          "spark-shell-version",
          "sqoop-version");

  @Nonnull
  @Override
  public String getName() {
    return "hadoop";
  }

  @Override
  public void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    SCRIPT_NAMES.stream().map(BashTask::new).forEach(out::add);
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    return new LocalHandle();
  }

  private class LocalHandle implements Handle {

    @Override
    public void close() {}
  }
}
