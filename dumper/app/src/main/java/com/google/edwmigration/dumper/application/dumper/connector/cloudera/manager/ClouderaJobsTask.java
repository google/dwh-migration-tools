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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The task dump job list from YARN and Cloudera Management API */
public class ClouderaJobsTask extends AbstractClouderaManagerTask {
  private static final Logger LOG = LoggerFactory.getLogger(ClouderaJobsTask.class);
  private static final DateTimeFormatter isoDateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  public ClouderaJobsTask() {
    super("jobs.jsonl");
  }

  @Override
  protected void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    String[] command = {
      "/bin/bash", "-c", "yarn application -list -appTypes MAPREDUCE -appStates ALL"
    };
    Process process = Runtime.getRuntime().exec(command);

    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String line;
    List<String> applicationIDs = new ArrayList<>();
    while ((line = reader.readLine()) != null) {
      String[] columns = line.split("\\s+");
      if (columns[0].contains("application")) {
        applicationIDs.add(columns[0]);
      }
    }

    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (String applicationID : applicationIDs) {
        String[] applicationDetailsCommand = {
          "/bin/bash", "-c", String.format("yarn application -status %s", applicationID),
        };
        Process applicationDetailsProcess = Runtime.getRuntime().exec(applicationDetailsCommand);
        reader =
            new BufferedReader(new InputStreamReader(applicationDetailsProcess.getInputStream()));
        while ((line = reader.readLine()) != null) {
          writer.write(line);
          writer.write('\n');
        }
      }
    }

    // String yarnURL =
    // "https://cldr3-data-hub-master0.cldr3-cd.svye-dcxb.a5.cloudera.site/cldr3-data-hub/cdp-proxy/yarnuiv2/ws/v1/cluster/apps";
    // CloseableHttpClient httpClient = handle.getHttpClient();
    // JsonNode chartInJson;
    // try (CloseableHttpResponse chart = httpClient.execute(new HttpGet(yarnURL))) {
    //   Scanner sc = new Scanner(chart.getEntity().getContent());
    //   while (sc.hasNext()) {
    //     LOG.warn(sc.nextLine());
    //   }
    //   LOG.warn(Integer.toString(chart.getStatusLine().getStatusCode()));
    //   chartInJson = readJsonTree(chart.getEntity().getContent());
    // } catch (IOException ex) {
    //   throw new RuntimeException(ex.getMessage(), ex);
    // }
    //
    // String stringifiedApplications = chartInJson.toString();
    // try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
    //   writer.write(stringifiedApplications);
    // }

    // String clusterName = "cldr3-data-hub";
    // String yarnApplicationsUrl =
    //     handle.getApiURI().toString()
    //         + "clusters/"
    //         + clusterName
    //         + "/services/yarn/yarnApplications";
    // String fromDate = buildISODateTime(100);
    // URI tsURI;
    // try {
    //   URIBuilder uriBuilder = new URIBuilder(yarnApplicationsUrl);
    //   uriBuilder.addParameter("limit", "100");
    //   uriBuilder.addParameter("from", fromDate);
    //   tsURI = uriBuilder.build();
    // } catch (URISyntaxException ex) {
    //   throw new TimeSeriesException(ex.getMessage(), ex);
    // }
    //
    // CloseableHttpClient httpClient = handle.getHttpClient();
    // JsonNode chartInJson;
    // try (CloseableHttpResponse chart = httpClient.execute(new HttpGet(tsURI))) {
    //   chartInJson = readJsonTree(chart.getEntity().getContent());
    // } catch (IOException ex) {
    //   throw new RuntimeException(ex.getMessage(), ex);
    // }
    //
    // String stringifiedApplications = chartInJson.toString();
    // LOG.warn(tsURI.toString());
    // try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
    //   writer.write(stringifiedApplications);
    // }

    // Configuration conf = new YarnConfiguration();
    // conf.addResource(
    //     new org.apache.hadoop.fs.Path(
    //         "/opt/cloudera/parcels/CDH/lib/hadoop/etc/hadoop/core-site.xml"));
    // conf.addResource(
    //     new org.apache.hadoop.fs.Path(
    //         "/opt/cloudera/parcels/CDH/lib/hadoop/etc/hadoop/hdfs-site.xml"));
    // conf.addResource(
    //     new org.apache.hadoop.fs.Path(
    //         "/opt/cloudera/parcels/CDH/lib/hadoop/etc/hadoop/yarn-site.xml"));
    //
    // YarnClient yarnClient = YarnClient.createYarnClient();
    // yarnClient.init(conf);
    // LOG.warn(conf.get("hadoop.security.authentication"));
    // yarnClient.start();
    //
    // List<ApplicationReport> applicationReports = yarnClient.getApplications();
    // applicationReports.forEach(
    //     appReport ->
    //         System.out.printf(
    //             "Job [%s]: id=%s, name=%s%n",
    //             appReport.getApplicationType(), appReport.getApplicationId(),
    // appReport.getName()));
    // yarnClient.stop();
  }

  private String buildISODateTime(int deltaInDays) {
    ZonedDateTime dateTime =
        ZonedDateTime.of(LocalDateTime.now().minusDays(deltaInDays), ZoneId.of("UTC"));
    return dateTime.format(isoDateTimeFormatter);
  }
}
