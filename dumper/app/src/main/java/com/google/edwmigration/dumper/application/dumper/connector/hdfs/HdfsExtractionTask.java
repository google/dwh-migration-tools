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
package com.google.edwmigration.dumper.application.dumper.connector.hdfs;

import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_HDFS_SCAN_ROOT_PATH;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_THREAD_POOL_SIZE;
import static com.google.edwmigration.dumper.application.dumper.connector.hdfs.SingleDirScanJob.trimExceptionMessage;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.concurrent.ExecutorManager;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.HdfsExtractionDumpFormat;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsExtractionTask extends AbstractTask<Void> implements HdfsExtractionDumpFormat {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsExtractionTask.class);

  private final int threadPoolSize;
  private final String hdfsScanRootPath;

  HdfsExtractionTask(@Nonnull ConnectorArguments args) {
    super(HdfsFormat.ZIP_ENTRY_NAME);
    threadPoolSize = args.getThreadPoolSize();
    Preconditions.checkArgument(
        threadPoolSize > 0, "Argument %s should be positive number", OPT_THREAD_POOL_SIZE);
    hdfsScanRootPath = args.getHdfsScanRootPath();
    Preconditions.checkArgument(
        !hdfsScanRootPath.isEmpty(), "Argument %s should be non-empty", OPT_HDFS_SCAN_ROOT_PATH);
  }

  @Override
  public String toString() {
    return format("Write HDFS metadata to %s", getTargetPath());
  }

  @Nonnull
  @Override
  public String getTargetPath() {
    return HdfsFormat.ZIP_ENTRY_NAME;
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    return TaskCategory.OPTIONAL;
  }

  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws IOException, ExecutionException, InterruptedException {
    DistributedFileSystem fs = ((HdfsHandle) handle).getDfs();
    // Create a dedicated ExecutorService to use:
    ExecutorService execService =
        ExecutorManager.newExecutorServiceWithBackpressure("hdfs-extraction", threadPoolSize);
    try (Writer output = sink.asCharSink(UTF_8).openBufferedStream();
        ExecutorManager execManager = new ExecutorManager(execService);
        ScanContext scanCtx = new ScanContext(execManager, fs, output)) {

      LOG.info(
          "Running HDFS extraction\n\t{}: {}\n\t{}: {}",
          OPT_HDFS_SCAN_ROOT_PATH,
          hdfsScanRootPath,
          OPT_THREAD_POOL_SIZE,
          threadPoolSize);
      FileStatus rootDir = fs.getFileStatus(new Path(hdfsScanRootPath));
      scanCtx.submitRootDirScanJob(rootDir, getContentSummaryFor(fs, rootDir));
      execManager.await(); // Wait until all (recursive) tasks are done executing
      LOG.info("Final stats:\n{}", scanCtx.getDetailedStats());
    } finally {
      // Shutdown the dedicated ExecutorService:
      MoreExecutors.shutdownAndAwaitTermination(execService, 100, TimeUnit.MILLISECONDS);
    }
    return null;
  }

  private ContentSummary getContentSummaryFor(DistributedFileSystem dfs, FileStatus file) {
    try {
      return dfs.getContentSummary(file.getPath());
    } catch (org.apache.hadoop.security.AccessControlException exn) {
      LOG.info(
          "Progress for HDFS extraction won't be displayed due to AccessControlException: {}",
          trimExceptionMessage(exn.getMessage()));
    } catch (IOException exn) {
      LOG.error("Progress for HDFS extraction won't be displayed due to IOException: ", exn);
    }
    return null;
  }
}
