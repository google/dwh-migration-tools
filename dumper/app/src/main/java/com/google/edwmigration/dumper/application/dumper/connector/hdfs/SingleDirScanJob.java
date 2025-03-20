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

import java.util.concurrent.Callable;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SingleDirScanJob is used solely and internally by class ScanContext. An instance of
 * SingleDirScanJob wraps a HDFS dir and is submitted to the ScanContext.execManager
 */
class SingleDirScanJob implements Callable<Void> {
  private static final Logger logger = LoggerFactory.getLogger(SingleDirScanJob.class);

  private final ScanContext scanCtx;
  private final FileStatus dir;

  SingleDirScanJob(ScanContext scanCtx, FileStatus dir) {
    this.scanCtx = scanCtx;
    this.dir = dir;
  }

  /** List directory's contents, trace/walk its files, and recursively walk its subdirs */
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
          scanCtx.startWalkDir(file);
        } else {
          numFiles++;
          scanCtx.walkFile(file);
        }
      }
      scanCtx.endWalkDir(dir, numFiles, numDirs, accumFileSize);
    } catch (org.apache.hadoop.security.AccessControlException exn) {
      logger.error("AccessControlException: {}", trimExceptionMessage(exn.getMessage()));
    } catch (Exception e) {
      logger.error(
          "Unexpected exception while scanning directory '{}'", dir.getPath().toUri().getPath(), e);
    }
    return null;
  }

  /**
   * Get rid of the stack trace part of the message, which should not be there anyway according to
   * the JVM spec.
   */
  static String trimExceptionMessage(String exnMessage) {
    int i = exnMessage.indexOf("\n\tat ");
    if (i > 0) {
      return exnMessage.substring(0, i).trim();
    }
    return exnMessage;
  }
}
