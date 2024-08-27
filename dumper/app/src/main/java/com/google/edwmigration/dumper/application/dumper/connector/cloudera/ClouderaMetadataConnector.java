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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.edwmigration.dumper.plugin.lib.dumper.spi.HadoopMetadataDumpFormat.FORMAT_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.hadoop.BashTask;
import com.google.edwmigration.dumper.application.dumper.connector.hadoop.HadoopInitializerTask;
import com.google.edwmigration.dumper.application.dumper.connector.hadoop.HadoopScripts;
import com.google.edwmigration.dumper.application.dumper.connector.hadoop.LocalFilesystemScanCommandGenerator;
import com.google.edwmigration.dumper.application.dumper.connector.meta.ChildConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from the Hadoop cluster via bash commands.")
public class ClouderaMetadataConnector implements MetadataConnector, ChildConnector {

  @VisibleForTesting
  static final ImmutableList<String> SCRIPT_NAMES =
      ImmutableList.of(
          "airflow-version",
          "apt-list-packages",
          "device-utilization-report",
          "docker-version",
          "gcc-version",
          "go-version",
          "gplusplus-version",
          "grafana-server-version",
          "hadoop-version",
          "hbase-shell-version",
          "hbase-version",
          "hive-version",
          "java-version",
          "lsb-release",
          "metastore-connection-details",
          "omd-version",
          "oozie-admin-status",
          "oozie-version",
          "perl-version",
          "pig-version",
          "python-version",
          "r-version",
          "rpm-list-packages",
          "ruby-version",
          "scala-version",
          "spark-shell-version",
          "spark-submit-version",
          "sqoop-version");

  public static final String CONNECTOR_NAME = "cloudera-metadata";

  private static final ImmutableList<String> SERVICE_NAMES =
      ImmutableList.of(
          "ntpd",
          "httpd",
          "named",
          "apache2",
          "ntp",
          "chronyd",
          "prometheus",
          "datadog-agent",
          "ganglia-monitor",
          "knox",
          "grafana-server",
          "crond");

  @Nonnull
  @Override
  public String getName() {
    return CONNECTOR_NAME;
  }

  @Override
  public void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception {
    out.add(new DumpMetadataTask(FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    SCRIPT_NAMES.stream()
        .map(scriptName -> new BashTask(scriptName, HadoopScripts.extract(scriptName + ".sh")))
        .forEach(out::add);
    out.addAll(generateTasksForSingleLineScripts());
    out.addAll(generateServiceScripts());
    out.add(
        new BashTask(
            "local-filesystem",
            HadoopScripts.create(
                "local-filesystem.sh",
                LocalFilesystemScanCommandGenerator.generate().getBytes(UTF_8))));
  }

  private ImmutableList<Task<?>> generateTasksForSingleLineScripts() {
    return HadoopScripts.extractSingleLineScripts().entrySet().stream()
        .map(entry -> new BashTask(entry.getKey(), entry.getValue()))
        .collect(toImmutableList());
  }

  private ImmutableList<Task<?>> generateServiceScripts() {
    return SERVICE_NAMES.stream()
        .flatMap(
            serviceName ->
                Stream.of(
                    generateBashTask(
                        serviceName + "-service-status",
                        String.format("service %s status", serviceName)),
                    generateBashTask(
                        serviceName + "-systemctl-status",
                        String.format("systemctl status %s", serviceName))))
        .collect(toImmutableList());
  }

  private BashTask generateBashTask(String scriptName, String command) {
    try {
      return new BashTask(
          scriptName,
          HadoopScripts.create(
              scriptName + ".sh", String.format("#!/bin/bash\n\n%s\n", command).getBytes(UTF_8)));
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Error generating the bash task for the script '%s'.", scriptName), e);
    }
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    return new LocalHandle();
  }

  @Nonnull
  @Override
  public Optional<Task<?>> createInitializerTask() {
    return Optional.of(new HadoopInitializerTask());
  }

  private class LocalHandle implements Handle {

    @Override
    public void close() {}
  }
}
