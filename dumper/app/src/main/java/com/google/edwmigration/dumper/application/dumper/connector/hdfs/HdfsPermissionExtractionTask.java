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

import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.concurrent.ExecutorManager;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.ExecutionException;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsPermissionExtractionTask implements Task<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsPermissionExtractionTask.class);

  final String clusterHost;
  final int port;
  final int poolSize;

  HdfsPermissionExtractionTask(@Nonnull String clusterHost, int port, int poolSize) {
    this.clusterHost = clusterHost;
    this.port = port;
    this.poolSize = poolSize;
  }

  @Override
  public String toString() {
    return format("Write hdfs permissions to %s", getTargetPath());
  }

  @Nonnull
  @Override
  public String getTargetPath() {
    return "hdfs-permissions.csv";
  }

  @CheckForNull
  @Override
  public Void run(TaskRunContext context) throws Exception {
    try (OutputHandle outputHandle = context.createOutputHandle(getTargetPath());
        Writer out = outputHandle.asTemporaryByteSink().asCharSink(UTF_8).openBufferedStream()) {
      doRun(out);
    }
    return null;
  }

  private void doRun(Writer output) throws IOException, ExecutionException, InterruptedException {
    LOG.info("clusterHost: {}", clusterHost);
    LOG.info("port: {}", port);
    LOG.info("threadPoolSize: {}", poolSize);

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://" + clusterHost + ":" + port + "/");

    try (FileSystem fs = FileSystem.get(conf);
        ScanContext scanCtx = new ScanContext(fs, output);
        ExecutorManager execManager =
            new ExecutorManager(
                ExecutorManager.newExecutorServiceWithBackpressure(
                    "hdfs-permission-extraction", poolSize))) {

      String hdfsPath = "/";
      FileStatus rootDir = fs.getFileStatus(new Path(hdfsPath));
      SingleDirScanJob rootJob = new SingleDirScanJob(scanCtx, execManager, rootDir);
      execManager.execute(rootJob); // The root job executes immediately
      execManager.await(); // Wait until all (recursive) tasks are done executing
      LOG.info(scanCtx.getFormattedStats());
    }
  }
}
