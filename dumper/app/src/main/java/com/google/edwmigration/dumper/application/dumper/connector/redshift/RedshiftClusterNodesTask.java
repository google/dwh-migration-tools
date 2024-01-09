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

import static java.util.stream.Collectors.toList;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.model.DescribeClustersRequest;
import com.amazonaws.services.redshift.model.DescribeClustersResult;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftMetadataDumpFormat.ClusterNodes;
import java.io.IOException;
import javax.annotation.Nonnull;

/** Extraction task to get information about Redshift Cluster nodes from AWS API. */
public class RedshiftClusterNodesTask extends AbstractAwsApiTask {

  public RedshiftClusterNodesTask(AWSCredentialsProvider credentialsProvider) {
    super(credentialsProvider, ClusterNodes.ZIP_ENTRY_NAME, ClusterNodes.Header.class);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, Handle handle)
      throws IOException {
    AmazonRedshift client = redshiftApiClient();
    DescribeClustersRequest request = new DescribeClustersRequest();
    DescribeClustersResult result = client.describeClusters(request);

    writeRecordsCsv(
        sink,
        result.getClusters().stream()
            .map(
                cluster ->
                    new Object[] {
                      cluster.getClusterIdentifier(),
                      cluster.getEndpoint() != null ? cluster.getEndpoint().getAddress() : "",
                      cluster.getNumberOfNodes(),
                      cluster.getNodeType(),
                      cluster.getTotalStorageCapacityInMegaBytes()
                    })
            .collect(toList()));
    return null;
  }
}
