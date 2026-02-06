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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaYarnApplicationDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.exception.SparkHistoryConnectionException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.exception.SparkLogFormatException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkYarnApplicationMetadata;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClouderaSparkYarnApplicationMetadataTask extends AbstractClouderaManagerTask {

  private static final Logger logger =
      LoggerFactory.getLogger(ClouderaSparkYarnApplicationMetadataTask.class);
  private static final int CONCURRENCY_LEVEL = 16;

  private final TaskCategory taskCategory;
  private final ExecutorService executor;

  private enum ExtractionResult {
    SUCCESS,
    NOT_FOUND,
    ERROR
  }

  public ClouderaSparkYarnApplicationMetadataTask(TaskCategory taskCategory) {
    super("yarn-application-spark-metadata.jsonl");
    Preconditions.checkNotNull(taskCategory, "Task category must be not null.");
    this.taskCategory = taskCategory;
    this.executor =
        Executors.newFixedThreadPool(
            CONCURRENCY_LEVEL,
            new ThreadFactoryBuilder().setNameFormat("spark-extractor-%d").build());
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    return taskCategory;
  }

  @Override
  protected void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    List<ClouderaClusterDTO> clusters = getClusters(handle);
    Map<String, List<String>> appIdsByCluster = getSparkAppIdsByCluster(handle);

    logger.info("Starting Spark Metadata Extraction with {} threads.", CONCURRENCY_LEVEL);
    try (JsonWriter writer = new JsonWriter(sink)) {
      for (ClouderaClusterDTO cluster : clusters) {
        processSingleCluster(
            context, cluster, appIdsByCluster.get(cluster.getName()), writer, handle);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private void processSingleCluster(
      TaskRunContext context,
      ClouderaClusterDTO cluster,
      List<String> appIds,
      JsonWriter writer,
      ClouderaManagerHandle handle) {

    String clusterName = cluster.getName();
    if (appIds == null || appIds.isEmpty()) return;

    SparkHistoryDiscoveryService discoveryService =
        new SparkHistoryDiscoveryService(
            objectMapper, handle.getClouderaManagerHttpClient(), handle.getApiURI());
    SparkJobMetadataExtractor extractor =
        new SparkJobMetadataExtractor(objectMapper, handle.getBasicAuthHttpClient());

    List<String> historyUrls =
        discoveryService.resolveUrl(
            clusterName,
            handle.getBasicAuthHttpClient(),
            context.getArguments().getSparkHistoryServiceNames());

    if (historyUrls.isEmpty()) {
      logger.warn("Skipping cluster '{}': History Server not found.", clusterName);
      return;
    }

    logger.info("Processing cluster '{}': Found {} applications.", clusterName, appIds.size());
    List<CompletableFuture<ExtractionResult>> tasks =
        submitExtractionTasks(appIds, historyUrls, extractor, writer, clusterName);

    logExecutionSummary(tasks, clusterName);
  }

  private List<CompletableFuture<ExtractionResult>> submitExtractionTasks(
      List<String> appIds,
      List<String> historyUrls,
      SparkJobMetadataExtractor extractor,
      JsonWriter writer,
      String clusterName) {

    AtomicInteger processedCount = new AtomicInteger(0);
    int totalApps = appIds.size();
    int logThreshold = Math.max(1, (int) (totalApps * 0.05));

    List<CompletableFuture<ExtractionResult>> tasks = new ArrayList<>();

    for (String appId : appIds) {
      tasks.add(
          CompletableFuture.supplyAsync(
              () -> {
                ExtractionResult result =
                    processApplication(extractor, writer, historyUrls, clusterName, appId);
                trackProgress(processedCount, totalApps, logThreshold, clusterName);
                return result;
              },
              executor));
    }
    return tasks;
  }

  private ExtractionResult processApplication(
      SparkJobMetadataExtractor extractor,
      JsonWriter writer,
      List<String> baseUrls,
      String clusterName,
      String appId) {
    for (String baseUrl : baseUrls) {
      try {
        String logUrl = String.format("%s/applications/%s/logs", baseUrl, appId);
        Optional<SparkYarnApplicationMetadata> metadata =
            extractor.extract(logUrl, clusterName, appId);
        if (metadata.isPresent()) {
          if (write(writer, metadata.get())) {
            return ExtractionResult.SUCCESS;
          } else {
            return ExtractionResult.ERROR;
          }
        }
      } catch (SparkHistoryConnectionException e) {
        logger.warn(
            "Could not fully verify App ID {} due to History Server connection issues.", appId);
        return ExtractionResult.ERROR;
      } catch (SparkLogFormatException e) {
        logger.warn("Corrupted log found for {} on {}: {}", appId, baseUrl, e.getMessage());
        return ExtractionResult.ERROR;
      }
    }
    return ExtractionResult.NOT_FOUND;
  }

  private synchronized boolean write(JsonWriter writer, SparkYarnApplicationMetadata metadata) {
    try {
      String jsonLine =
          serializeObjectToJsonString(
              ImmutableMap.of("yarnAppSparkMetadata", ImmutableList.of(metadata)));
      writer.writeLine(jsonLine);
      return true;
    } catch (IOException e) {
      logger.error(
          "Failed to write metadata for app {}: {}", metadata.getApplicationId(), e.getMessage());
      return false;
    }
  }

  private void trackProgress(AtomicInteger counter, int total, int threshold, String clusterName) {
    int current = counter.incrementAndGet();
    if (current % threshold == 0 || current == total) {
      double percent = (double) current / total * 100;
      logger.info(
          "Progress for cluster '{}': {}/{} ({}%)",
          clusterName, current, total, String.format("%.0f", percent));
    }
  }

  private void logExecutionSummary(
      List<CompletableFuture<ExtractionResult>> tasks, String clusterName) {
    try {
      // join() waits for all threads to finish
      List<ExtractionResult> results =
          tasks.stream().map(CompletableFuture::join).collect(Collectors.toList());

      long successCount = results.stream().filter(r -> r == ExtractionResult.SUCCESS).count();
      long missingCount = results.stream().filter(r -> r == ExtractionResult.NOT_FOUND).count();
      long errorCount = results.stream().filter(r -> r == ExtractionResult.ERROR).count();

      logger.info(
          "Finished processing Spark applications of cluster '{}'. Success: {}, Not Found: {}, Errors: {}",
          clusterName,
          successCount,
          missingCount,
          errorCount);

    } catch (Exception e) {
      logger.error("Error waiting for tasks to complete for cluster " + clusterName, e);
    }
  }

  private List<ClouderaClusterDTO> getClusters(ClouderaManagerHandle handle) {
    List<ClouderaClusterDTO> clusters = handle.getClusters();
    if (clusters == null) {
      throw new MetadataDumperUsageException(
          "Clusters must be initialized before fetching Spark YARN applications metadata.");
    }
    return clusters;
  }

  private Map<String, List<String>> getSparkAppIdsByCluster(ClouderaManagerHandle handle) {
    List<ClouderaYarnApplicationDTO> sparkYarnApplications = handle.getSparkYarnApplications();
    if (sparkYarnApplications == null) {
      throw new MetadataDumperUsageException(
          "Spark YARN applications must be initialized before fetching Spark YARN application metadata.");
    }
    return sparkYarnApplications.stream()
        .collect(
            Collectors.groupingBy(
                ClouderaYarnApplicationDTO::getClusterName,
                Collectors.mapping(ClouderaYarnApplicationDTO::getId, Collectors.toList())));
  }
}
