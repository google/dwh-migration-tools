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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.concurrent.ExecutorManager;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.HdfsPermissionExtractionDumpFormat;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsPermissionExtractionTask extends AbstractTask<Void>
    implements HdfsPermissionExtractionDumpFormat {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsPermissionExtractionTask.class);

  final String clusterHost;
  final int port;
  final int poolSize;

  HdfsPermissionExtractionTask(@Nonnull ConnectorArguments args) {
    super(PermissionExtraction.ZIP_ENTRY_NAME);
    Preconditions.checkNotNull(args, "Arguments was null.");
    clusterHost = Preconditions.checkNotNull(args.getHost(), "Host was null.");
    port = args.getPort(/* defaultPort= */ 8020);
    poolSize = args.getThreadPoolSize();
  }

  @Override
  public String toString() {
    return format("Write hdfs permissions to %s", getTargetPath());
  }

  @Nonnull
  @Override
  public String getTargetPath() {
    return PermissionExtraction.ZIP_ENTRY_NAME;
  }

  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws IOException, ExecutionException, InterruptedException {
    LOG.info("clusterHost: {}", clusterHost);
    LOG.info("port: {}", port);
    LOG.info("threadPoolSize: {}", poolSize);

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://" + clusterHost + ":" + port + "/");
    FileSystem fs = FileSystem.get(conf);
    checkArgument(
        fs instanceof DistributedFileSystem,
        "Not a DistributedFileSystem - can't create ScanContext.");

    // Create a dedicated ExecutorService to use:
    ExecutorService execService =
        ExecutorManager.newExecutorServiceWithBackpressure("hdfs-permission-extraction", poolSize);
    try (Writer output = sink.asCharSink(UTF_8).openBufferedStream();
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        ScanContext scanCtx = new ScanContext(dfs, output);
        ExecutorManager execManager = new ExecutorManager(execService)) {

      String hdfsPath = "/";
      FileStatus rootDir = fs.getFileStatus(new Path(hdfsPath));
      SingleDirScanJob rootJob = new SingleDirScanJob(scanCtx, execManager, rootDir);
      execManager.execute(rootJob); // The root job executes immediately
      execManager.await(); // Wait until all (recursive) tasks are done executing
      LOG.info(scanCtx.getFormattedStats());
    } finally {
      // Shutdown the dedicated ExecutorService:
      MoreExecutors.shutdownAndAwaitTermination(execService, 100, TimeUnit.MILLISECONDS);
    }
    return null;
  }
}
