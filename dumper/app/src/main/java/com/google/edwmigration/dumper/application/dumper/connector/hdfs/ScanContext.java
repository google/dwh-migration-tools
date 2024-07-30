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
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.HdfsPermissionExtractionDumpFormat.PermissionExtraction;
import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.WillClose;
import javax.annotation.concurrent.GuardedBy;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

final class ScanContext implements Closeable {

  private static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

  private final DistributedFileSystem dfs;
  private final DFSClient dfsClient;
  private final CSVPrinter csvPrinter;
  private final Instant instantScanBegin;
  private LongAdder timeSpentInListStatus = new LongAdder();
  private LongAdder numFilesByListStatus = new LongAdder();

  @GuardedBy("csvPrinter")
  private long accumulatedFileSize = 0L;

  @GuardedBy("csvPrinter")
  private long numFiles = 0L;

  @GuardedBy("csvPrinter")
  private long numDirs = 0L;

  @GuardedBy("csvPrinter")
  private long numDirsWalked = 0L;

  ScanContext(DistributedFileSystem dfs, @WillClose Writer outputSink) throws IOException {
    this.dfs = dfs;
    this.dfsClient = dfs.getClient();
    this.csvPrinter =
        AbstractTask.FORMAT.withHeader(PermissionExtraction.Header.class).print(outputSink);
    this.instantScanBegin = Instant.now();
  }

  @Override
  public void close() throws IOException {
    csvPrinter.close();
  }

  FileStatus[] listDirectory(FileStatus dir) throws IOException {
    Instant instantListBegin = Instant.now();
    FileStatus[] files = dfs.listStatus(dir.getPath());
    timeSpentInListStatus.add(Duration.between(instantListBegin, Instant.now()).toMillis());
    numFilesByListStatus.add(files.length);
    return files;
  }

  void beginWalkDir(FileStatus dir) {}

  /*
   * CsvPrint the directory attributes of the specified dir
   * and incrementally update the scan statistics (this is what the rest of the parameters are for)
   */
  void endWalkDir(FileStatus dir, long nFiles, long nDirs, long accumFileSize) throws IOException {
    String absolutePath = dir.getPath().toUri().getPath();
    HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(absolutePath);
    String strModificationTime =
        DATE_FORMAT.format(Instant.ofEpochMilli(dir.getModificationTime()));
    byte byteStoragePolicy = hdfsFileStatus.getStoragePolicy();
    StoragePolicy storagePolicy = StoragePolicy.valueOf(byteStoragePolicy);
    String strStoragePolicy =
        storagePolicy != null ? storagePolicy.toString() : String.valueOf(byteStoragePolicy);

    synchronized (csvPrinter) {
      this.numDirsWalked++;
      this.numFiles += nFiles;
      this.numDirs += nDirs;
      this.accumulatedFileSize += accumFileSize;

      csvPrinter.printRecord(
          absolutePath,
          "D", // for a directory
          accumFileSize, // Size of a directory is the sum of sizes of (immediately) contained files
          dir.getOwner(),
          dir.getGroup(),
          dir.getPermission(),
          strModificationTime,
          nFiles,
          nDirs,
          strStoragePolicy);
    }
  }

  void walkFile(FileStatus file) throws IOException {
    String absolutePath = file.getPath().toUri().getPath();
    HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(absolutePath);
    String strModificationTime =
        DATE_FORMAT.format(Instant.ofEpochMilli(file.getModificationTime()));
    byte byteStoragePolicy = hdfsFileStatus.getStoragePolicy();
    StoragePolicy storagePolicy = StoragePolicy.valueOf(byteStoragePolicy);
    String strStoragePolicy =
        storagePolicy != null ? storagePolicy.toString() : String.valueOf(byteStoragePolicy);

    synchronized (csvPrinter) {
      csvPrinter.printRecord(
          absolutePath,
          "F", // for a file
          file.getLen(),
          file.getOwner(),
          file.getGroup(),
          file.getPermission(),
          strModificationTime,
          /* nFiles= */ 0,
          /* nDirs= */ 0,
          strStoragePolicy);
    }
  }

  /** This method is used to produce meaningful metrics for log/debug purposes. */
  String getFormattedStats() {
    final Duration timeSinceScanBegin = Duration.between(instantScanBegin, Instant.now());
    long numFilesByListStatus = this.numFilesByListStatus.longValue();
    Duration timeSpentInListStatus = Duration.ofMillis(this.timeSpentInListStatus.longValue());
    Duration avgTimeSpentInListStatusPerFile =
        numFilesByListStatus > 0
            ? timeSpentInListStatus.dividedBy(numFilesByListStatus)
            : Duration.ZERO;

    final long numFilesDivisor = numFiles > 0 ? numFiles : 1;
    StringBuilder sb =
        new StringBuilder()
            .append("[HDFS Permission extraction stats]")
            .append("\nTotal: num files&dirs: " + numFiles)
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
            .append(timeSinceScanBegin.dividedBy(numFilesDivisor).toMillis() + "ms\n")
            .append("\n[/HDFS Permission extraction stats]");

    return sb.toString();
  }
}
