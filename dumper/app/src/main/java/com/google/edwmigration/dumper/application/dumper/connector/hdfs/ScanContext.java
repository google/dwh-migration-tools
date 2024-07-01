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

import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

final class ScanContext implements Closeable {

  private static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

  private final FileSystem fs;
  private final DFSClient dfsClient;
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
    ModificationTime,
    NumberOfFilesAndSubdirs,
    NumberOfSubdirs,
    StoragePolicy,
  }

  ScanContext(FileSystem fs, Writer outputSink) throws IOException {
    checkArgument(
        fs instanceof DistributedFileSystem,
        "Not a DistributedFileSystem - can't create ScanContext.");

    this.fs = fs;
    this.dfsClient = ((DistributedFileSystem) fs).getClient();
    this.outputSink = outputSink;
    this.csvPrinter = AbstractTask.FORMAT.withHeader(CsvHeader.class).print(outputSink);
    this.instantScanBegin = Instant.now();
  }

  @Override
  public void close() throws IOException {
    csvPrinter.close();
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

      String absolutePath = dir.getPath().toUri().getPath();
      HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(absolutePath);
      byte byteStoragePolicy = hdfsFileStatus.getStoragePolicy();
      StoragePolicy storagePolicy = StoragePolicy.valueOf(byteStoragePolicy);

      csvPrinter.printRecord(
          absolutePath,
          dir.getOwner(),
          dir.getGroup(),
          dir.getPermission(),
          DATE_FORMAT.format(Instant.ofEpochMilli(dir.getModificationTime())),
          nFiles,
          nDirs,
          storagePolicy != null ? storagePolicy.toString() : String.valueOf(byteStoragePolicy));
    }
  }

  void walkFile(FileStatus file) {}

  /** This method is used to produce meaningful metrics for log/debug purposes. */
  String getFormattedStats() {

    final Duration timeSinceScanBegin = Duration.between(instantScanBegin, Instant.now());
    Duration avgTimeSpentInListStatusPerFile;
    long secondsSpentInListStatus;
    synchronized (LOCK1) {
      avgTimeSpentInListStatusPerFile =
          numFilesByListStatus > 0
              ? timeSpentInListStatus.dividedBy(numFilesByListStatus)
              : Duration.ZERO;
      secondsSpentInListStatus = timeSpentInListStatus.getSeconds();
    }
    final long numFilesDivisor = numFiles > 0 ? numFiles : 1;
    StringBuilder sb =
        new StringBuilder()
            .append("Total: num files&dirs: " + numFiles)
            .append("\n       num dirs found: " + numDirs)
            .append("\n       num dirs walkd: " + numDirsWalked)
            .append("\nTotal File Size: " + accumulatedFileSize)
            .append("\nAvg File Size: " + accumulatedFileSize / numFilesDivisor)
            .append("\nTotal time: ")
            .append(timeSinceScanBegin.getSeconds() + "s")
            .append("\nTotal time in listStatus(..): ")
            .append(secondsSpentInListStatus + "s")
            .append("\nAvg time per file in listStatus(..): ")
            .append(avgTimeSpentInListStatusPerFile.toMillis() + "ms")
            .append("\nAvg time per doc: ")
            .append(timeSinceScanBegin.toMillis() / numFilesDivisor + "ms\n");

    return sb.toString();
  }
}
