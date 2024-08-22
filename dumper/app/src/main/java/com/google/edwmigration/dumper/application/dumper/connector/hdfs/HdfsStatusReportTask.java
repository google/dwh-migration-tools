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
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.HdfsPermissionExtractionDumpFormat.StatusReport;
import java.io.IOException;
import java.io.Writer;
import javax.annotation.Nonnull;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;

public class HdfsStatusReportTask extends AbstractTask<Void> implements StatusReport {

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  HdfsStatusReportTask() {
    super(ZIP_ENTRY_NAME);
  }

  @Override
  public String toString() {
    return format("Write HDFS status report to %s", getTargetPath());
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

    public static HdfsStatus create(DistributedFileSystem dfs) throws IOException {
      return new AutoValue_HdfsStatusReportTask_HdfsStatus(
          dfs.getStatus(),
          dfs.setSafeMode(SafeModeAction.GET),
          dfs.getBytesWithFutureGenerationStamps(),
          HdfsNameNodeStatus.create(dfs.getClient().getNamenode()),
          HdfsDataNodesStatus.create(dfs));
    }
  }

  @AutoValue
  abstract static class HdfsNameNodeStatus {
    @JsonProperty
    abstract ReplicatedBlockStats replicatedBlockStats();

    @JsonProperty
    abstract ECBlockGroupStats ecBlockGroupStats();

    public static HdfsNameNodeStatus create(ClientProtocol namenode) throws IOException {
      return new AutoValue_HdfsStatusReportTask_HdfsNameNodeStatus(
          namenode.getReplicatedBlockStats(), namenode.getECBlockGroupStats());
    }
  }

  @AutoValue
  abstract static class HdfsDataNodesStatus {
    @JsonProperty
    abstract ImmutableList<DatanodeInfo> regularNodes();

    @JsonProperty
    abstract ImmutableList<DatanodeInfo> slowNodes();

    public static HdfsDataNodesStatus create(DistributedFileSystem dfs) throws IOException {
      return new AutoValue_HdfsStatusReportTask_HdfsDataNodesStatus(
          ImmutableList.copyOf(dfs.getDataNodeStats()),
          ImmutableList.copyOf(dfs.getSlowDatanodeStats()));
    }
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws IOException {
    DistributedFileSystem dfs = ((HdfsHandle) handle).getDfs();
    HdfsStatus status = HdfsStatus.create(dfs);
    try (final Writer output = sink.asCharSink(UTF_8).openBufferedStream()) {
      OBJECT_MAPPER.writeValue(output, status);
    }
    return null;
  }
}
