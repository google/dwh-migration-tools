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
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.concurrent.ExecutorManager;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsContentSummaryTask implements Task<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsContentSummaryTask.class);

  private final String clusterHost;
  private final int port;

  private enum CsvHeader {
    Path,
    TotalSubtreeFileSize,
    TotalSubtreeNumberOfFiles,
  }

  HdfsContentSummaryTask(@Nonnull String clusterHost, int port) {
    this.clusterHost = clusterHost;
    this.port = port;
  }

  @Override
  public String toString() {
    return format(
        "Write content summary of the top level directories of a hdfs %s", getTargetPath());
  }

  @Nonnull
  @Override
  public String getTargetPath() {
    return "hdfs-content-summary.csv";
  }

  @CheckForNull
  @Override
  public Void run(TaskRunContext context) throws Exception {
    try (OutputHandle outputHandle = context.createOutputHandle(getTargetPath());
        Writer out = outputHandle.asTemporaryByteSink().asCharSink(UTF_8).openBufferedStream()) {
      doRun(out, context.getExecutorService());
    }
    return null;
  }

  private void doRun(Writer output, Executor executorService)
      throws IOException, ExecutionException, InterruptedException {
    LOG.info("clusterHost: {}", clusterHost);
    LOG.info("port: {}", port);

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://" + clusterHost + ":" + port + "/");
    FileSystem fs = FileSystem.get(conf);

    try (final DistributedFileSystem dfs = (DistributedFileSystem) fs;
        final ExecutorManager execManager = new ExecutorManager(executorService);
        final CSVPrinter csvPrinter =
            AbstractTask.FORMAT.withHeader(CsvHeader.class).print(output)) {

      String hdfsPath = "/";
      FileStatus rootDir = fs.getFileStatus(new Path(hdfsPath));
      FileStatus[] topLevelFiles = fs.listStatus(rootDir.getPath());
      for (FileStatus file : topLevelFiles) {
        // Process file or dir (in this case - just collect statistics)
        if (file.isDirectory()) {
          execManager.submit(
              new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                  ContentSummary summary = fs.getContentSummary(file.getPath());
                  long totalFileSize = summary.getLength(); // This gives you the total size
                  long totalNumberOfFiles = summary.getFileCount();
                  long totalNumberOfDirectories = summary.getDirectoryCount();
                  synchronized (csvPrinter) {
                    csvPrinter.printRecord(
                        file.getPath().toUri().getPath(),
                        totalFileSize,
                        totalNumberOfDirectories + totalNumberOfFiles);
                  }
                  return null;
                }
              });
        }
      }
      execManager.await(); // Wait until all submitted tasks are done executing
    }
  }
}
