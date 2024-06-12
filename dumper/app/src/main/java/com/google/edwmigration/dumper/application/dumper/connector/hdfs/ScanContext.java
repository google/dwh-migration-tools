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

import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import java.io.IOException;
import java.io.Writer;
import java.time.Duration;
import java.time.Instant;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

final class ScanContext {

  private final FileSystem fs;
  private final Writer outputSink;
  private final CSVPrinter csvPrinter;
  private final Instant instantScanBegin;
  private Duration timeSpentInListStatus = Duration.ofMillis(0);
  private long numFilesByListStatus = 0L;
  private long accumulatedFileSize = 0L;
  private long numFiles = 0L;
  private long numDirs = 0L;
  private long numDirsWalked = 0L;
  private final Object LOCK1 = new Object();

  private enum CsvHeader {
    Path,
    Owner,
    Group,
    Permission,
  }

  ScanContext(FileSystem fs, Writer outputSink) throws IOException {
    this.fs = fs;
    this.outputSink = outputSink;
    this.csvPrinter = AbstractTask.FORMAT.withHeader(CsvHeader.class).print(outputSink);
    this.instantScanBegin = Instant.now();
  }

  FileStatus[] listDirectory(FileStatus dir) throws IOException {
    Instant instantListBegin = Instant.now();
    FileStatus[] files = fs.listStatus(dir.getPath());
    synchronized (LOCK1) {
      timeSpentInListStatus =
          timeSpentInListStatus.plus(Duration.between(instantListBegin, Instant.now()));
      numFilesByListStatus += files.length;
    }
    return files;
  }

  void beginWalkDir(FileStatus dir) {}

  void endWalkDir(FileStatus dir, long nFiles, long nDirs, long accumFileSize) throws IOException {
    synchronized (outputSink) {
      this.numDirsWalked++;
      this.numFiles += nFiles;
      this.numDirs += nDirs;
      this.accumulatedFileSize += accumFileSize;

      csvPrinter.printRecord(
          // skip the schema, host and port, get the path only:
          dir.getPath().toUri().getPath(), dir.getOwner(), dir.getGroup(), dir.getPermission());
    }
  }

  /** This method is used to produce meaningful metrics for log/debug purposes. */
  String getFormattedStats() {
    final Duration timeSinceScanBegin = Duration.between(instantScanBegin, Instant.now());
    final Duration avgTimeSpentInListStatusPerFile =
        numFilesByListStatus > 0
            ? timeSpentInListStatus.dividedBy(numFilesByListStatus)
            : Duration.ZERO;
    final long numFilesDivisor = numFiles > 0 ? numFiles : 1;
    StringBuilder sb =
        new StringBuilder()
            .append("Total: num files/dirs: " + numFiles)
            .append("\n       num dirs found: " + numDirs)
            .append("\n       num dirs walkd: " + numDirsWalked)
            .append("\nTotal File Size: " + accumulatedFileSize)
            .append("\nAvg File Size: " + accumulatedFileSize / numFilesDivisor)
            .append("\nTotal time: ")
            .append(timeSinceScanBegin.getSeconds() + "s")
            .append("\nTotal time in listStatus(..): ")
            .append(timeSpentInListStatus.getSeconds() + "s")
            .append("\nAvg time per file in listStatus(..): ")
            .append(avgTimeSpentInListStatusPerFile.toMillis() + "ms")
            .append("\nAvg time per doc: ")
            .append(timeSinceScanBegin.toMillis() / numFilesDivisor + "ms\n");

    return sb.toString();
  }
}
