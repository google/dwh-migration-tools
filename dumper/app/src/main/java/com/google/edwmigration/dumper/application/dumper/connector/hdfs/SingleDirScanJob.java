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

import com.google.edwmigration.dumper.plugin.ext.jdk.concurrent.ExecutorManager;
import java.util.concurrent.Callable;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SingleDirScanJob implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(SingleDirScanJob.class);

  private final ScanContext scanCtx;
  private final ExecutorManager execManager;
  private final FileStatus dir;

  SingleDirScanJob(ScanContext scanCtx, ExecutorManager execManager, FileStatus dir) {
    this.scanCtx = scanCtx;
    this.dir = dir;
    this.execManager = execManager;
  }

  @Override
  public Void call() {
    long numFiles = 0;
    long numDirs = 0;
    long accumFileSize = 0;

    try {
      for (FileStatus file : scanCtx.listDirectory(dir)) {
        // Process file or dir (in this case - just collect statistics)
        accumFileSize += file.getLen();

        if (file.isDirectory()) {
          numDirs++;
          execManager.submit(new SingleDirScanJob(scanCtx, execManager, file));
        } else {
          numFiles++;
          scanCtx.walkFile(file);
        }
      }
      scanCtx.endWalkDir(dir, numFiles, numDirs, accumFileSize);
    } catch (Exception e) {
      LOG.error(
          "Unexpected exception while scanning HDFS folder '{}'",
          dir.getPath().toUri().getPath(),
          e);
    }
    return null;
  }
}
