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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.AbstractClouderaTimeSeriesTask.TimeSeriesAggregation.*;
import static com.google.edwmigration.dumper.application.dumper.task.TaskCategory.*;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.net.URI;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;

@AutoService({Connector.class})
@Description("Dumps metadata from Cloudera Manager.")
@RespectsInput(
    order = 100,
    arg = ConnectorArguments.OPT_URI,
    description = "The Cloudera's Manager API HTTP(s) endpoint.")
@RespectsInput(
    order = 200,
    arg = ConnectorArguments.OPT_USER,
    description = "The username for Cloudera's Manager.")
@RespectsInput(
    order = 300,
    arg = ConnectorArguments.OPT_PASSWORD,
    description = "The password for Cloudera's Manager.",
    required = "If not specified as an argument, will use a secure prompt")
@RespectsInput(
    order = 400,
    arg = ConnectorArguments.OPT_CLUSTER,
    description = "The name of Cloudera's cluster.",
    required = "Only if you need to dump data for a single Cloudera cluster")
public class ClouderaManagerConnector extends AbstractConnector {

  private static final String FORMAT_NAME = "cloudera-manager.dump.zip";

  public ClouderaManagerConnector() {
    super("cloudera-manager");
  }

  @Nonnull
  @Override
  public String getDefaultFileName(boolean isAssessment, Clock clock) {
    return ArchiveNameUtil.getFileNameWithTimestamp(getName(), clock);
  }

  @Override
  public void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    out.add(new ClouderaClustersTask());
    out.add(new ClouderaCMFHostsTask());
    out.add(new ClouderaAPIHostsTask());
    out.add(new ClouderaServicesTask());
    out.add(new ClouderaHostComponentsTask());

    if (arguments.getStartDate() == null && arguments.getEndDate() == null) {
      out.addAll(defaultTasks());
    } else {
      out.addAll(customDateRangeTasks(arguments.getStartDate(), arguments.getEndDate()));
    }
  }

  private List<? extends AbstractClouderaManagerTask> defaultTasks() {
    ZonedDateTime today = ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("UTC"));
    ZonedDateTime startDate = today.minusDays(90);
    return ImmutableList.of(
        (new ClouderaClusterCPUChartTask.Builder())
            .setOutputFileName("cluster-cpu-90d.jsonl")
            .setStartDate(startDate)
            .setEndDate(today)
            .setTsAggregation(DAILY)
            .setTaskCategory(OPTIONAL)
            .build(),
        (new ClouderaHostRAMChartTask.Builder())
            .setOutputFileName("host-ram-90d.jsonl")
            .setStartDate(startDate)
            .setEndDate(today)
            .setTsAggregation(DAILY)
            .setTaskCategory(OPTIONAL)
            .build(),
        new ClouderaYarnApplicationsTask("yarn-applications-90d.jsonl", startDate, today, OPTIONAL),
        new ClouderaYarnApplicationTypeTask(
            "yarn-application-types-90d.jsonl", startDate, today, OPTIONAL));
  }

  private List<? extends Task<?>> customDateRangeTasks(
      ZonedDateTime startDate, @Nullable ZonedDateTime endDate) {
    return ImmutableList.of(
        (new ClouderaClusterCPUChartTask.Builder())
            .setOutputFileName("cluster-cpu-custom-date-range.jsonl")
            .setStartDate(startDate)
            .setEndDate(endDate)
            .setTsAggregation(DAILY)
            .setTaskCategory(REQUIRED)
            .build(),
        (new ClouderaHostRAMChartTask.Builder())
            .setOutputFileName("host-ram-custom-date-range.jsonl")
            .setStartDate(startDate)
            .setEndDate(endDate)
            .setTsAggregation(DAILY)
            .setTaskCategory(REQUIRED)
            .build(),
        new ClouderaYarnApplicationsTask(
            "yarn-applications-custom-date-range.jsonl", startDate, endDate, OPTIONAL),
        new ClouderaYarnApplicationTypeTask(
            "yarn-application-types-custom-date-range.jsonl", startDate, endDate, OPTIONAL));
  }

  @Nonnull
  @Override
  public ClouderaManagerHandle open(@Nonnull ConnectorArguments arguments) throws Exception {
    URI uri = new URI(arguments.getUri());
    CloseableHttpClient httpClient = disableSSLVerification(HttpClients.custom()).build();
    ClouderaManagerHandle handle = new ClouderaManagerHandle(uri, httpClient);

    String user = arguments.getUser();
    String password = arguments.getPasswordOrPrompt();
    doClouderaManagerLogin(handle.getBaseURI(), httpClient, user, password);
    return handle;
  }

  @Override
  public void validate(ConnectorArguments arguments) {
    String clouderaUri = arguments.getUri();
    Preconditions.checkNotNull(clouderaUri, "--url for Cloudera Manager API is required");

    String clouderaUser = arguments.getUser();
    Preconditions.checkNotNull(
        clouderaUser, "--user is required for Cloudera Manager API connector");

    validateDateRange(arguments);
  }

  private void doClouderaManagerLogin(
      URI baseURI, CloseableHttpClient httpClient, String user, String password) throws Exception {
    ClouderaManagerLoginHelper.login(baseURI, httpClient, user, password);
  }

  private HttpClientBuilder disableSSLVerification(HttpClientBuilder builder) throws Exception {
    // Cloudera Manager API SSL certificate is not in list of know certificates.
    // So, switch off SSL certificate validation.
    // It is  expected that Dumper will work  in internal private network (probably localhost calls)
    builder.setSSLContext(
        new SSLContextBuilder().loadTrustMaterial(null, TrustAllStrategy.INSTANCE).build());
    builder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
    return builder;
  }
}
