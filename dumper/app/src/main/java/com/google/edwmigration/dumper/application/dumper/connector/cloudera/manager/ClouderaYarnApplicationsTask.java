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
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiYARNApplicationDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The task dumps YARN applications from Cloudera Manager API */
public class ClouderaYarnApplicationsTask extends AbstractClouderaYarnApplicationTask {
  private static final Logger logger = LoggerFactory.getLogger(ClouderaYarnApplicationsTask.class);

  public ClouderaYarnApplicationsTask(int days, TaskCategory taskCategory) {
    super("yarn-applications", days, taskCategory);
  }

  @Override
  protected void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    List<ClouderaClusterDTO> clusters = handle.getClusters();
    Preconditions.checkNotNull(
        clusters, "Clusters must be initialized before fetching YARN applications.");

    PaginatedClouderaYarnApplicationsLoader appLoader =
        new PaginatedClouderaYarnApplicationsLoader(
            handle, context.getArguments().getPaginationPageSize());

    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ClouderaClusterDTO cluster : clusters) {
        String clusterName = cluster.getName();
        logger.info("Dump YARN applications from {} cluster", clusterName);
        int loadAppsCnt =
            appLoader.load(
                clusterName,
                yarnAppsPage -> writeYarnApplications(writer, yarnAppsPage, clusterName));
        logger.info("Dumped {} YARN applications from {} cluster", loadAppsCnt, clusterName);
      }
    }
  }

  private void writeYarnApplications(
      Writer writer, List<ApiYARNApplicationDTO> yarnApps, String clusterName) {
    for (ApiYARNApplicationDTO yarnApp : yarnApps) {
      yarnApp.setClusterName(clusterName);
    }
    try {
      String yarnAppsJson = serializeObjectToJsonString(ImmutableMap.of("yarnApps", yarnApps));
      writer.write(yarnAppsJson);
      writer.write('\n');
    } catch (IOException ex) {
      throw new ClouderaConnectorException("Can't dump YARN applications.", ex);
    }
  }
}
