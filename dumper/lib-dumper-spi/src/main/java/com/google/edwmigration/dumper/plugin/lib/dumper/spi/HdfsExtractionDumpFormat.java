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
package com.google.edwmigration.dumper.plugin.lib.dumper.spi;

public interface HdfsExtractionDumpFormat {
  String FORMAT_NAME = "hdfs.zip";

  interface HdfsFormat {
    String ZIP_ENTRY_NAME = "hdfs.csv";

    enum Header {
      Path,
      FileType, // "F" for a file, "D" for a directory
      FileSize, // In case of directory it is the sum of sizes of all (immediately) contained files
      Owner,
      Group,
      Permission,
      ModificationTime,
      FileCount,
      DirCount,
      StoragePolicy,
    }
  }

  interface ContentSummaryFormat {
    String ZIP_ENTRY_NAME = "hdfs-content-summary.csv";

    enum Header {
      Path,
      TotalSubtreeFileSize,
      TotalSubtreeNumberOfFiles,
    }
  }

  interface StatusReport {
    String ZIP_ENTRY_NAME = "hdfs-status-report.json";
  }
}