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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ArrayUtils.insert;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.DescribeClustersRequest;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftRawLogsDumpFormat;
import java.io.IOException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/** Extraction task to get Redshift time series metrics from AWS CloudWatch API. */
public class RedshiftClusterUsageMetricsTask extends AbstractAwsApiTask {

  protected static enum MetricName {
    CPUUtilization,
    PercentageDiskSpaceUsed
  }

  protected static enum MetricType {
    Average
  }

  @AutoValue
  protected abstract static class MetricConfig {

    public abstract MetricName name();

    public abstract MetricType type();

    public static MetricConfig create(MetricName name, MetricType type) {
      return new AutoValue_RedshiftClusterUsageMetricsTask_MetricConfig(name, type);
    }
  }

  @AutoValue
  protected abstract static class MetricDataPoint {

    public abstract Date date();

    public abstract Double value();

    public static MetricDataPoint create(Date date, Double value) {
      return new AutoValue_RedshiftClusterUsageMetricsTask_MetricDataPoint(date, value);
    }
  }

  private static final String REDSHIFT_NAMESPACE = "AWS/Redshift";
  private static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);

  private final ZonedDateTime currentTime;
  private final ZonedInterval interval;
  private final List<MetricConfig> metrics;

  public RedshiftClusterUsageMetricsTask(
      AWSCredentialsProvider credentialsProvider,
      ZonedDateTime currentTime,
      ZonedInterval interval,
      String zipEntryName,
      ImmutableList<MetricConfig> metrics) {
    super(
        credentialsProvider,
        zipEntryName,
        RedshiftRawLogsDumpFormat.ClusterUsageMetrics.Header.class);
    this.interval = interval;
    this.metrics = metrics;
    this.currentTime = currentTime;
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, Handle handle)
      throws IOException {
    writeRecordsCsv(
        sink,
        listClusters().stream()
            .flatMap(
                cluster ->
                    zipMetricStreams(
                            metrics.stream()
                                .map(
                                    metricConfig ->
                                        getMetricDataPoints(
                                            cluster.getClusterIdentifier(), metricConfig))
                                .collect(toImmutableList()))
                        .stream()
                        .map(metricData -> insert(0, metricData, cluster.getClusterIdentifier())))
            .collect(toList()));
    return null;
  }

  private List<Cluster> listClusters() {
    AmazonRedshift client = redshiftApiClient();
    return client.describeClusters(new DescribeClustersRequest()).getClusters();
  }

  private ImmutableList<MetricDataPoint> getMetricDataPoints(
      String clusterId, MetricConfig metricConfig) {
    AmazonCloudWatch client = cloudWatchApiClient();
    GetMetricStatisticsRequest request =
        new GetMetricStatisticsRequest()
            .withMetricName(metricConfig.name().name())
            .withStatistics(metricConfig.type().name())
            .withNamespace(REDSHIFT_NAMESPACE)
            .withDimensions(new Dimension().withName("ClusterIdentifier").withValue(clusterId))
            .withStartTime(Date.from(interval.getStartUTC().toInstant()))
            .withEndTime(Date.from(interval.getEndExclusiveUTC().toInstant()))
            .withPeriod((int) metricDataPeriod().getSeconds());

    GetMetricStatisticsResult result = client.getMetricStatistics(request);
    return result.getDatapoints().stream()
        .map(
            datapoint ->
                MetricDataPoint.create(
                    datapoint.getTimestamp(), getDatapointValue(metricConfig, datapoint)))
        .collect(toImmutableList());
  }

  private Double getDatapointValue(MetricConfig metricConfig, Datapoint datapoint) {
    switch (metricConfig.type()) {
      case Average:
        return datapoint.getAverage();
      default:
        return null;
    }
  }

  private ImmutableList<Object[]> zipMetricStreams(
      List<ImmutableList<MetricDataPoint>> metricsCollection) {
    SortedMap<String, Double[]> metricsMap = new TreeMap<String, Double[]>();

    for (int i = 0; i < metricsCollection.size(); i++) {
      Integer metricIndex = Integer.valueOf(i);
      metricsCollection
          .get(metricIndex)
          .forEach(
              dataPoint -> {
                String dateString = DATE_FORMAT.format(dataPoint.date().toInstant());
                if (!metricsMap.containsKey(dateString)) {
                  metricsMap.put(dateString, new Double[metricsCollection.size()]);
                }
                metricsMap.get(dateString)[metricIndex] = dataPoint.value();
              });
    }

    return metricsMap.keySet().stream()
        .map(
            metricDate -> {
              return Stream.of(new Object[] {metricDate}, metricsMap.get(metricDate))
                  .flatMap(Stream::of)
                  .toArray();
            })
        .collect(toImmutableList());
  }

  /**
   * Returns available metric period based on the interval time.
   * https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html
   */
  private Duration metricDataPeriod() {
    ZonedDateTime start = interval.getStartUTC();
    if (start.isAfter(currentTime.minusDays(14))) {
      return Duration.ofMinutes(1);
    }
    if (start.isAfter(currentTime.minusDays(62))) {
      return Duration.ofMinutes(5);
    }
    return Duration.ofHours(1);
  }
}
