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
package com.google.edwmigration.dumper.application.dumper.connector.hdfs;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.HdfsPermissionExtractionDumpFormat.StatusReport;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsStatusReportTask extends AbstractTask<Void> implements StatusReport {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsStatusReportTask.class);

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  HdfsStatusReportTask() {
    super(ZIP_ENTRY_NAME);
  }

  @Override
  public String toString() {
    return format("Write HDFS status report to %s", getTargetPath());
  }

  @Override
  public TaskCategory getCategory() {
    return TaskCategory.OPTIONAL;
  }

  @AutoValue
  abstract static class HdfsStatus {
    @JsonProperty
    abstract FsStatus fsStatus();

    @JsonProperty
    abstract boolean safeMode();

    @JsonProperty
    abstract long bytesWithFutureGenerationStamps();

    @JsonProperty
    abstract HdfsNameNodeStatus nameNode();

    @JsonProperty
    abstract HdfsDataNodesStatus dataNodes();

    public static HdfsStatus create(
        FsStatus fsStatus,
        boolean safeMode,
        long bytesWithFutureGenerationStamps,
        HdfsNameNodeStatus nameNode,
        HdfsDataNodesStatus dataNodes) {
      return new AutoValue_HdfsStatusReportTask_HdfsStatus(
          fsStatus, safeMode, bytesWithFutureGenerationStamps, nameNode, dataNodes);
    }
  }

  @AutoValue
  abstract static class HdfsNameNodeStatus {
    @JsonProperty
    abstract ReplicatedBlockStats replicatedBlockStats();

    @JsonProperty
    abstract ECBlockGroupStats ecBlockGroupStats();

    public static HdfsNameNodeStatus create(
        ReplicatedBlockStats replicatedBlockStats, ECBlockGroupStats ecBlockGroupStats) {
      return new AutoValue_HdfsStatusReportTask_HdfsNameNodeStatus(
          replicatedBlockStats, ecBlockGroupStats);
    }
  }

  @AutoValue
  abstract static class HdfsDataNodesStatus {
    @JsonProperty("regular")
    abstract ImmutableList<DatanodeInfo> regularNodes();

    @JsonProperty("slow")
    abstract ImmutableList<DatanodeInfo> slowNodes();

    public static HdfsDataNodesStatus create(
        List<DatanodeInfo> regularNodes, List<DatanodeInfo> slowNodes) {
      return new AutoValue_HdfsStatusReportTask_HdfsDataNodesStatus(
          ImmutableList.copyOf(regularNodes), ImmutableList.copyOf(slowNodes));
    }
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws IOException {
    DistributedFileSystem dfs = ((HdfsHandle) handle).getDfs();
    HdfsStatus status = extractHdfsStatus(dfs);
    try (final Writer output = sink.asCharSink(UTF_8).openBufferedStream()) {
      OBJECT_MAPPER.writeValue(output, status);
    }
    return null;
  }

  private HdfsStatus extractHdfsStatus(DistributedFileSystem dfs) throws IOException {
    return HdfsStatus.create(
        dfs.getStatus(),
        dfs.setSafeMode(SafeModeAction.GET),
        dfs.getBytesWithFutureGenerationStamps(),
        extractNameNode(dfs.getClient().getNamenode()),
        extractDataNodes(dfs));
  }

  private HdfsNameNodeStatus extractNameNode(ClientProtocol namenode) throws IOException {
    return HdfsNameNodeStatus.create(
        namenode.getReplicatedBlockStats(), namenode.getECBlockGroupStats());
  }

  private HdfsDataNodesStatus extractDataNodes(DistributedFileSystem dfs) {
    ImmutableList<DatanodeInfo> regularNodes;
    ImmutableList<DatanodeInfo> slowNodes;
    try {
      regularNodes = ImmutableList.copyOf(dfs.getDataNodeStats());
    } catch (Exception ex) {
      LOG.warn("Error retrieving data node stats.", ex);
      regularNodes = ImmutableList.of();
    }

    try {
      slowNodes = ImmutableList.copyOf(dfs.getSlowDatanodeStats());
    } catch (Exception ex) {
      LOG.warn("Error retrieving slow data node stats.", ex);
      slowNodes = ImmutableList.of();
    }
    return HdfsDataNodesStatus.create(regularNodes, slowNodes);
  }
}
