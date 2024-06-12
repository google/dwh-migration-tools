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

import java.util.concurrent.RecursiveTask;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SingleDirScanJob extends RecursiveTask<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(SingleDirScanJob.class);

  private final ScanContext scanCtx;
  private final AtomicCounter jobsTodoCounter;
  private final FileStatus dir;

  SingleDirScanJob(ScanContext scanCtx, AtomicCounter jobsTodoCounter, FileStatus dir) {
    this.scanCtx = scanCtx;
    this.dir = dir;
    this.jobsTodoCounter = jobsTodoCounter;
    jobsTodoCounter.increment();
  }

  @Override
  protected Void compute() throws IllegalStateException {
    long numFiles = 0;
    long numDirs = 0;
    long accumFileSize = 0;

    try {
      scanCtx.beginWalkDir(dir);
      for (FileStatus file : scanCtx.listDirectory(dir)) {
        // Process file or dir (in this case - just collect statistics)
        accumFileSize += file.getLen();
        numFiles++;

        if (file.isDirectory()) {
          numDirs++;
          new SingleDirScanJob(scanCtx, jobsTodoCounter, file).fork();
        } else {
          scanCtx.walkFile(file);
        }
      }
      scanCtx.endWalkDir(dir, numFiles, numDirs, accumFileSize);
    } catch (Exception exn) {
      LOG.error(
          "Unexpected exception while scanning HDFS folder " + dir.getPath().toUri().getPath(),
          exn);
    } finally {
      jobsTodoCounter.decrement();
    }
    return null;
  }
}
