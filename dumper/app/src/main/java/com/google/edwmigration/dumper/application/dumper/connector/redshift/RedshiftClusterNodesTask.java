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

import static com.google.edwmigration.dumper.application.dumper.SummaryPrinter.joinSummaryDoubleLine;

import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.DescribeClustersRequest;
import com.amazonaws.services.redshift.model.DescribeClustersResult;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.RedshiftHandle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftMetadataDumpFormat.ClusterNodes;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;

/** Extraction task to get information about Redshift Cluster nodes from AWS API. */
public class RedshiftClusterNodesTask extends AbstractAwsApiTask {

  public RedshiftClusterNodesTask() {
    super(ClusterNodes.ZIP_ENTRY_NAME, ClusterNodes.Header.class);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, Handle handle)
      throws IOException {
    if (!(handle instanceof RedshiftHandle)) {
      throw new IllegalStateException("Redshift handle was expected but got " + handle);
    }
    RedshiftHandle redshiftHandle = (RedshiftHandle) handle;

    // If the user did not provide AWS credentials, the client will not be present.
    // In this case, we can simply skip the execution of this task.
    if (!redshiftHandle.getRedshiftClient().isPresent()) {
      return null;
    }
    AmazonRedshift redshiftClient = redshiftHandle.getRedshiftClient().get();

    dumpClusterNodes(redshiftClient, sink);
    return null;
  }

  private void dumpClusterNodes(AmazonRedshift redshiftClient, @Nonnull ByteSink sink)
      throws IOException {
    CSVFormat format = FORMAT.builder().setHeader(headerEnum).build();
    try (CsvRecordWriter writer = new CsvRecordWriter(sink, format, getName())) {
      DescribeClustersRequest request = new DescribeClustersRequest();
      String marker = null;
      do {
        request.setMarker(marker);
        DescribeClustersResult result = redshiftClient.describeClusters(request);
        for (Cluster item : result.getClusters()) {
          writer.handleRecord(
              item.getClusterIdentifier(),
              item.getEndpoint() != null ? item.getEndpoint().getAddress() : "",
              item.getNumberOfNodes(),
              item.getNodeType(),
              item.getTotalStorageCapacityInMegaBytes());
        }
        marker = result.getMarker();
      } while (marker != null);
    }
  }

  private String toCallDescription() {
    return "AmazonRedshift.describeClusters";
  }

  @Override
  public String describeSourceData() {
    return joinSummaryDoubleLine(
        "Write " + ClusterNodes.ZIP_ENTRY_NAME + " from AWS API request:", toCallDescription());
  }
}
