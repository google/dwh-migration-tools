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

import static com.google.edwmigration.dumper.application.dumper.TasksRunner.PROGRESS_LOG;

import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.plugin.ext.jdk.concurrent.ExecutorManager;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.HdfsExtractionDumpFormat.HdfsFormat;
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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A ScanContext is used to walk & scan (possibly in parallel) a HDFS or parts of it. */
final class ScanContext implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ScanContext.class);
  private static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
  private static final boolean PROGRESS_DEBUG_STATS = false;
  /** In PROD mode update progress every 60s, In DEBUG mode update stats every 10s */
  private static final long PROGRESS_UPDATE_INTERVAL =
      PROGRESS_DEBUG_STATS ? 10_000 : 60_000; // millis

  private final ExecutorManager execManager;
  private final DistributedFileSystem dfs;
  private final DFSClient dfsClient;
  private final CSVPrinter csvPrinter;
  private final Instant instantScanBegin;
  private LongAdder timeSpentInListStatus = new LongAdder();
  private LongAdder numFilesByListStatus = new LongAdder();
  private LongAdder numCallsToListStatus = new LongAdder();

  @GuardedBy("csvPrinter")
  private volatile boolean hasContentSummary = false;

  private LongAdder numFilesByContentSummary = new LongAdder();
  private LongAdder numDirsByContentSummary = new LongAdder();

  @GuardedBy("csvPrinter")
  private long accumulatedFileSize = 0L;

  @GuardedBy("csvPrinter")
  private long numFiles = 0L;

  @GuardedBy("csvPrinter")
  private long numDirs = 0L;

  @GuardedBy("csvPrinter")
  private long numDirsWalked = 0L;

  /**
   * The only constructor.
   *
   * @param execManager the execution manager used to recursively perform the scan
   * @param dfs the HDFS (parts of which are) going to be scanned
   * @param outputSink where the results of the scan are written
   * @throws IOException if something is wrong with the outputSink
   */
  ScanContext(ExecutorManager execManager, DistributedFileSystem dfs, @WillClose Writer outputSink)
      throws IOException {
    this.execManager = execManager;
    this.dfs = dfs;
    this.dfsClient = dfs.getClient();
    this.csvPrinter = AbstractTask.FORMAT.withHeader(HdfsFormat.Header.class).print(outputSink);
    this.instantScanBegin = Instant.now();
  }

  /** Flush/close the output sink */
  @Override
  public void close() throws IOException {
    csvPrinter.close();
  }

  /** Virtually a private method - should be referenced only by SingleDirScanJob.call */
  FileStatus[] listDirectory(FileStatus dir) throws IOException {
    Instant instantListBegin = Instant.now();
    FileStatus[] files = dfs.listStatus(dir.getPath());
    timeSpentInListStatus.add(Duration.between(instantListBegin, Instant.now()).toMillis());
    numFilesByListStatus.add(files.length);
    numCallsToListStatus.increment();
    return files;
  }

  /**
   * The entry point of the ScanContext API - submits a (root) directory to be scanned and returns
   * immediately. Multiple roots may be submitted. It does not make sense if one of them is
   * (recursively) a subdir of another one. Use execManager.await() if you want to wait for the scan
   * to complete.
   *
   * @param dir a hdfs directory to be scanned recursively. It should belong to the dfs file system.
   */
  public void submitRootDirScanJob(FileStatus dir, ContentSummary contentSummary) {
    if (contentSummary != null) {
      synchronized (csvPrinter) {
        hasContentSummary = true;
        numFilesByContentSummary.add(contentSummary.getFileCount());
        numDirsByContentSummary.add(contentSummary.getDirectoryCount());
      }
    }
    startWalkDir(dir);
  }

  /** Submits a recursive job for the specified dir to the execManager. Updates some HDFS stats */
  void startWalkDir(FileStatus dir) {
    synchronized (csvPrinter) {
      this.numDirs++; // numDirs(Found) >= numDirsWalked
      this.accumulatedFileSize += dir.getLen();
    }
    execManager.submit(new SingleDirScanJob(this, dir));
  }

  /** Exports the attributes of the specified dir to the sink. Updates some HDFS stats */
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

      maybeLogStats(dir);
    }
  }

  /** Exports the attributes of the specified file to the sink. Updates some HDFS stats */
  void walkFile(FileStatus file) throws IOException {
    String absolutePath = file.getPath().toUri().getPath();
    HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(absolutePath);
    if (hdfsFileStatus == null) {
      LOG.error("Unable to scan file {}", absolutePath);
      return;
    }
    String strModificationTime =
        DATE_FORMAT.format(Instant.ofEpochMilli(file.getModificationTime()));
    byte byteStoragePolicy = hdfsFileStatus.getStoragePolicy();
    StoragePolicy storagePolicy = StoragePolicy.valueOf(byteStoragePolicy);
    String strStoragePolicy =
        storagePolicy != null ? storagePolicy.toString() : String.valueOf(byteStoragePolicy);

    synchronized (csvPrinter) {
      this.numFiles++; // numFiles(Found) == numFilesWalked
      this.accumulatedFileSize += file.getLen();
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

      maybeLogStats(file);
    }
  }

  @GuardedBy("csvPrinter")
  private long lastTimeStatsLogged;

  private void maybeLogStats(FileStatus file) {
    // TODO get rid of System.currentTimeMillis() call
    // TODO consider using  [Concurrent]RecordProgressMonitor for progress logging
    // TODO see Shevek's comments to https://github.com/google/dwh-migration-tools/pull/534
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastTimeStatsLogged >= PROGRESS_UPDATE_INTERVAL) {
      lastTimeStatsLogged = currentTime;
      if (PROGRESS_DEBUG_STATS) {
        // Don't log the path in production as it may be considered 'sensitive information'!
        LOG.info("path scanned: {}\n{}", file.getPath().toUri().getPath(), getDetailedStats());
      } else {
        PROGRESS_LOG.info("{}", getProgressMessage());
      }
    }
  }

  /** Produce meaningful HDFS stats for logging/debug purposes. */
  public String getDetailedStats() {
    final Duration timeSinceScanBegin = Duration.between(instantScanBegin, Instant.now());

    Duration timeSpentInListStatus = Duration.ofMillis(this.timeSpentInListStatus.longValue());

    long numFilesByListStatus = this.numFilesByListStatus.longValue();
    Duration avgTimeSpentInListStatusPerFile =
        numFilesByListStatus > 0
            ? timeSpentInListStatus.dividedBy(numFilesByListStatus)
            : Duration.ZERO;

    long numCallsToListStatus = this.numCallsToListStatus.longValue();
    Duration avgTimeSpentInListStatusPerCall =
        numCallsToListStatus > 0
            ? timeSpentInListStatus.dividedBy(numCallsToListStatus)
            : Duration.ZERO;

    // Synchronize to copy thread-shared state:
    long numFiles, numDirs, numDirsWalked, accumulatedFileSize;
    synchronized (csvPrinter) {
      numFiles = this.numFiles;
      numDirs = this.numDirs;
      numDirsWalked = this.numDirsWalked;
      accumulatedFileSize = this.accumulatedFileSize;
    }
    final long totalNumFiles = numFiles > 0 ? numFiles : 1;
    final long totalFilesAndDirs = numFiles + numDirs > 0 ? numFiles + numDirs : 1;

    String percentFiles =
        numFilesByContentSummary.longValue() != 0
            ? ((numFiles * 100) / numFilesByContentSummary.longValue()) + "%"
            : "100%";
    String percentDirsFound =
        numDirsByContentSummary.longValue() != 0
            ? ((numDirs * 100) / numDirsByContentSummary.longValue()) + "%"
            : "100%";
    String percentDirsWalkd =
        numDirsByContentSummary.longValue() != 0
            ? ((numDirsWalked * 100) / numDirsByContentSummary.longValue()) + "%"
            : "100%";

    if (!hasContentSummary) {
      percentFiles = percentDirsFound = percentDirsWalkd = "--";
    }

    String stats =
        "[HDFS extraction stats]"
            + f("\nTotal: num files     : %s\t(%s)", numFiles, percentFiles)
            + f("\n       num dirs found: %s\t(%s)", numDirs, percentDirsFound)
            + f("\n       num dirs walkd: %s\t(%s)", numDirsWalked, percentDirsWalkd)
            + f("\n    file size scanned: %s", accumulatedFileSize)
            + f("\navg file size scanned: %s", accumulatedFileSize / totalNumFiles)
            + f("\n      user time spent: %ss", timeSinceScanBegin.getSeconds())
            + f(
                "\n  avg user time / doc: %sms",
                timeSinceScanBegin.dividedBy(totalFilesAndDirs).toMillis())
            + "\nDistributedFileSystem.listStatus(..) stats: "
            + f("\n\t total cpu time spent: %ss", timeSpentInListStatus.getSeconds())
            + f("\n\t    avg time per file: %sms", avgTimeSpentInListStatusPerFile.toMillis())
            + f("\n\t    avg time per call: %sms", avgTimeSpentInListStatusPerCall.toMillis())
            + "\n[/HDFS extraction stats]";
    return stats;
  }

  /** Produce meaningful Progress indicator message. */
  public String getProgressMessage() {
    if (!hasContentSummary) {
      return f("HDFS extraction - %s files done", numFiles + numDirsWalked);
    }

    long filesToDo = numFilesByContentSummary.longValue() + numDirsByContentSummary.longValue();
    if (filesToDo == 0) {
      filesToDo = 1;
    }
    long filesDone = numFiles + numDirsWalked;
    long secondsSinceScanBegin = Duration.between(instantScanBegin, Instant.now()).getSeconds();
    double percentDone = (100.0 * filesDone) / filesToDo;
    String percentDoneString =
        filesDone == filesToDo ? "100" : f(percentDone < 10.0 ? "%.2f" : "%.1f", percentDone);
    return f(
        "HDFS extraction - %s%% Completed (%s file%s done in %.2fm)",
        percentDoneString, filesDone, (filesDone > 1 ? "s" : ""), secondsSinceScanBegin / 60.0);
  }

  private static String f(String format, Object... args) {
    return String.format(format, args);
  }
}
