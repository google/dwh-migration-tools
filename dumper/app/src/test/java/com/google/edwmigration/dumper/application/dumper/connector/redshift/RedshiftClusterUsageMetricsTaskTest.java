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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.DescribeClustersResult;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.redshift.RedshiftClusterUsageMetricsTask.MetricConfig;
import com.google.edwmigration.dumper.application.dumper.connector.redshift.RedshiftClusterUsageMetricsTask.MetricName;
import com.google.edwmigration.dumper.application.dumper.connector.redshift.RedshiftClusterUsageMetricsTask.MetricType;
import com.google.edwmigration.dumper.application.dumper.handle.RedshiftHandle;
import com.google.edwmigration.dumper.application.dumper.task.MemoryByteSink;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftRawLogsDumpFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class RedshiftClusterUsageMetricsTaskTest {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private RedshiftHandle redshiftHandle;

  @Mock private AmazonRedshift redshiftClientMock;

  @Mock private AmazonCloudWatch cloudWatchClientMock;

  private static final ImmutableList<Cluster> TEST_CLUSTERS =
      ImmutableList.of(
          new Cluster().withClusterIdentifier("clId1"),
          new Cluster().withClusterIdentifier("clId2"));

  private static final ZonedDateTime CURR_DATE_TIME =
      ZonedDateTime.of(2024, 01, 02, 11, 33, 44, 55, ZoneId.of("UTC"));
  private static final ZonedInterval TEST_INTERVAL =
      new ZonedInterval(
          ZonedDateTime.of(2024, 01, 02, 03, 0, 44, 55, ZoneId.of("UTC")),
          ZonedDateTime.of(2024, 01, 02, 03, 10, 44, 55, ZoneId.of("UTC")));
  private static final String TEST_ZIP_ENTRY_NAME = "cluster_metrics.csv";

  @Test
  public void doRun_success() throws Exception {
    ImmutableList<MetricConfig> testMetrics =
        ImmutableList.of(
            MetricConfig.create(MetricName.CPUUtilization, MetricType.Average),
            MetricConfig.create(MetricName.PercentageDiskSpaceUsed, MetricType.Average));
    Class<? extends Enum<?>> testHeader =
        RedshiftRawLogsDumpFormat.ClusterUsageMetrics.Header.class;
    Date metricDate1 = Date.from(TEST_INTERVAL.getStartUTC().toInstant());
    Date metricDate2 = Date.from(TEST_INTERVAL.getStartUTC().plusMinutes(1).toInstant());
    Date metricDate3 = Date.from(TEST_INTERVAL.getStartUTC().plusMinutes(2).toInstant());
    Date metricDate4 = Date.from(TEST_INTERVAL.getStartUTC().plusMinutes(3).toInstant());
    Date metricDate5 = Date.from(TEST_INTERVAL.getStartUTC().plusMinutes(4).toInstant());
    GetMetricStatisticsRequest expectedRequestCpu1 =
        createExpectedRequest("CPUUtilization", "Average", "clId1");
    GetMetricStatisticsRequest expectedRequestCpu2 =
        createExpectedRequest("CPUUtilization", "Average", "clId2");
    GetMetricStatisticsRequest expectedRequestStorage1 =
        createExpectedRequest("PercentageDiskSpaceUsed", "Average", "clId1");
    GetMetricStatisticsRequest expectedRequestStorage2 =
        createExpectedRequest("PercentageDiskSpaceUsed", "Average", "clId2");
    GetMetricStatisticsResult resultCpu1 =
        createCloudWatchResult(
            new Datapoint().withTimestamp(metricDate1).withAverage(10.5),
            new Datapoint().withTimestamp(metricDate2).withAverage(11.5));
    GetMetricStatisticsResult resultCpu2 =
        createCloudWatchResult(
            new Datapoint().withTimestamp(metricDate3).withAverage(12.5),
            new Datapoint().withTimestamp(metricDate4).withAverage(13.5));
    GetMetricStatisticsResult resultStorage1 =
        createCloudWatchResult(
            new Datapoint().withTimestamp(metricDate1).withAverage(14.5),
            new Datapoint().withTimestamp(metricDate2).withAverage(15.5));
    GetMetricStatisticsResult resultStorage2 =
        createCloudWatchResult(
            new Datapoint().withTimestamp(metricDate3).withAverage(16.5),
            new Datapoint().withTimestamp(metricDate4).withAverage(17.5),
            new Datapoint().withTimestamp(metricDate5).withAverage(18.5));

    when(redshiftHandle.getRedshiftClient()).thenReturn(Optional.of(redshiftClientMock));
    when(redshiftHandle.getCloudWatchClient()).thenReturn(Optional.of(cloudWatchClientMock));
    when(redshiftClientMock.describeClusters(any()))
        .thenReturn(new DescribeClustersResult().withClusters(TEST_CLUSTERS));
    when(cloudWatchClientMock.getMetricStatistics(expectedRequestCpu1)).thenReturn(resultCpu1);
    when(cloudWatchClientMock.getMetricStatistics(expectedRequestCpu2)).thenReturn(resultCpu2);
    when(cloudWatchClientMock.getMetricStatistics(expectedRequestStorage1))
        .thenReturn(resultStorage1);
    when(cloudWatchClientMock.getMetricStatistics(expectedRequestStorage2))
        .thenReturn(resultStorage2);

    MemoryByteSink sink = new MemoryByteSink();

    RedshiftClusterUsageMetricsTask task =
        new RedshiftClusterUsageMetricsTask(CURR_DATE_TIME, TEST_INTERVAL, TEST_ZIP_ENTRY_NAME);

    task.doRun(null, sink, redshiftHandle);

    String actualOutput = sink.openStream().toString();

    assertEquals(
        "cluster_identifier,interval_time,cpu_avg,storage_avg\n"
            + "clId1,2024-01-02 03:00:44.000,10.5,14.5\n"
            + "clId1,2024-01-02 03:01:44.000,11.5,15.5\n"
            + "clId2,2024-01-02 03:02:44.000,12.5,16.5\n"
            + "clId2,2024-01-02 03:03:44.000,13.5,17.5\n"
            + "clId2,2024-01-02 03:04:44.000,,18.5\n",
        actualOutput);
  }

  private GetMetricStatisticsRequest createExpectedRequest(
      String metricName, String metricType, String clusterId) {
    return new GetMetricStatisticsRequest()
        .withMetricName(metricName)
        .withStatistics(metricType)
        .withNamespace("AWS/Redshift")
        .withDimensions(new Dimension().withName("ClusterIdentifier").withValue(clusterId))
        .withStartTime(Date.from(TEST_INTERVAL.getStartUTC().toInstant()))
        .withEndTime(Date.from(TEST_INTERVAL.getEndExclusiveUTC().toInstant()))
        .withPeriod(60);
  }

  private GetMetricStatisticsResult createCloudWatchResult(Datapoint... datapoints) {
    return new GetMetricStatisticsResult().withDatapoints(datapoints);
  }
}
