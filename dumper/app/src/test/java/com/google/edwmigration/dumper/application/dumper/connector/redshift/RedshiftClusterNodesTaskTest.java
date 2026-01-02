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

import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.DescribeClustersResult;
import com.amazonaws.services.redshift.model.Endpoint;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.handle.RedshiftHandle;
import com.google.edwmigration.dumper.application.dumper.task.MemoryByteSink;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class RedshiftClusterNodesTaskTest {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private AmazonRedshift redshiftClientMock;
  @Mock private RedshiftHandle redshiftHandle;

  @Test
  public void doRun_success() throws Exception {
    when(redshiftHandle.getRedshiftClient()).thenReturn(Optional.of(redshiftClientMock));

    when(redshiftClientMock.describeClusters(any()))
        .thenReturn(
            new DescribeClustersResult()
                .withClusters(
                    ImmutableList.of(
                        new Cluster()
                            .withClusterIdentifier("clId1")
                            .withEndpoint(new Endpoint().withAddress("hostAddr1"))
                            .withNumberOfNodes(3)
                            .withNodeType("ra3.4xlarge")
                            .withTotalStorageCapacityInMegaBytes(35000L),
                        new Cluster()
                            .withClusterIdentifier("clId2")
                            .withEndpoint(new Endpoint().withAddress("hostAddr2"))
                            .withNumberOfNodes(1)
                            .withNodeType("ra3.16xlarge")
                            .withTotalStorageCapacityInMegaBytes(45000L))));

    MemoryByteSink sink = new MemoryByteSink();

    RedshiftClusterNodesTask task = new RedshiftClusterNodesTask();
    task.doRun(null, sink, redshiftHandle);

    String actualOutput = sink.openStream().toString();
    assertEquals(
        "cluster_identifier,host,nodes_num,node_type,total_storage\n"
            + "clId1,hostAddr1,3,ra3.4xlarge,35000\n"
            + "clId2,hostAddr2,1,ra3.16xlarge,45000\n",
        actualOutput);
  }
}
