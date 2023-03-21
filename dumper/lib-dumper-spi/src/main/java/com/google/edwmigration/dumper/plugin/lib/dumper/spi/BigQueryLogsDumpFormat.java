/*
 * Copyright 2022-2023 Google LLC
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import javax.annotation.CheckForNull;

/** @author matt */
public interface BigQueryLogsDumpFormat {

  ObjectMapper MAPPER = BigQueryMetadataDumpFormat.MAPPER;
  String FORMAT_NAME = "bigquery.logs.zip";

  interface QueryLogsTask {

    String ZIP_ENTRY_NAME = "query_history.jsonl";

    enum Header {
      ProjectId,
      /** The default dataset (=schema) name when this query was run. */
      DatasetId,
      UserId,
      /** Milliseconds UTC. */
      StartTime,
      /** Milliseconds UTC. */
      EndTime,
      Query,
      Result, // SUCCESS, ERROR
      ErrorResult,
      BillingTier,
      CacheHit,
      EstimatedBytesProcessed,
      TotalBytesBilled,
      TotalBytesProcessed,
      TotalSlotMilliseconds,
      DmlAffectedRowCount,
      TotalPartitionsProcessed,
    }
  }

  interface QueryHistoryJson {

    String ZIP_ENTRY_NAME = "query_history.jsonl";

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    class TableId {

      @CheckForNull public String project;
      @CheckForNull public String dataset;
      @CheckForNull public String table;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    class JobStatus {

      /** From JobStatus.BigQueryError. */
      @CheckForNull public String message;
      /** From JobStatus.BigQueryError. */
      @CheckForNull public String reason;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    class QueryStatistics {

      @CheckForNull public Integer billingTier;
      @CheckForNull public Boolean cacheHit;
      @CheckForNull public Long estimatedBytesProcessed;
      @CheckForNull public Long totalBytesBilled;
      @CheckForNull public Long totalBytesProcessed;
      @CheckForNull public Long totalSlotMilliseconds;
      @CheckForNull public Long dmlAffectedRowCount;
      @CheckForNull public Long totalPartitionsProcessed;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    class QueryJobConfiguration {

      /** The project in which the job was executed. */
      @CheckForNull public String project;
      /** The default dataset in which the job was executed. */
      @CheckForNull public String defaultDataset;

      @CheckForNull public String userEmail;
      @CheckForNull public String query;

      // Where the data is written.
      @CheckForNull public TableId destinationTable;
      @CheckForNull public String createDisposition;
      @CheckForNull public String writeDisposition;

      // What were the statistics
      @CheckForNull public QueryStatistics statistics;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    class LoadJobConfiguration {
      @CheckForNull public List<String> sourceUris;
      @CheckForNull public String sourceFormat;
      @CheckForNull public String createDisposition;
      @CheckForNull public String writeDisposition;
      // Where the data is loaded.
      @CheckForNull public TableId destinationTable;

      @CheckForNull public LoadStatistics statistics;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    class LoadStatistics {
      @CheckForNull public Long badRecords;
      @CheckForNull public Long inputBytes;
      @CheckForNull public Long inputFiles;
      @CheckForNull public Long outputBytes;
      @CheckForNull public Long outputRows;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    class CopyJobConfiguration {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    class ExtractJobConfiguration {}

    // BigQueryAuditMetadata.JobChange -> BigQueryAuditMetadata.Job
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    class Job {

      @CheckForNull public String project;
      @CheckForNull public String job;
      @CheckForNull public Map<String, String> labels;
      @CheckForNull public String userEmail;
      @CheckForNull public JobStatus jobStatus;
      /** In milliseconds since epoch. */
      @CheckForNull public Long startTime;
      /** In milliseconds since epoch. */
      @CheckForNull public Long endTime;

      @CheckForNull public CopyJobConfiguration copyJobConfiguration;
      @CheckForNull public ExtractJobConfiguration extractJobConfiguration;
      @CheckForNull public LoadJobConfiguration loadJobConfiguration;
      @CheckForNull public QueryJobConfiguration queryJobConfiguration;
    }
  }
}
