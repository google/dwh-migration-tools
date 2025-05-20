-- Copyright 2022-2025 Google LLC
-- Copyright 2013-2021 CompilerWorks
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
SELECT
    NULL "ConId",
    SqlStat.dbid "Dbid",
    SqlStat.instance_number "InstanceNumber",
    to_char(SqlStat.force_matching_signature) "ForceMatchingSignature",
    min(SqlStat.sql_id) "SqlId",
    min(Snapshot.begin_interval_time) "MinBeginTime",
    max(Snapshot.end_interval_time) "MaxEndTime",
    sum(SqlStat.apwait_delta) "ApWaitTotal",
    sum(SqlStat.buffer_gets_delta) "BufferGetsTotal",
    sum(SqlStat.ccwait_delta) "CcWaitTotal",
    sum(SqlStat.clwait_delta) "ClWaitTotal",
    sum(SqlStat.cpu_time_delta) "CpuTimeTotal",
    sum(SqlStat.direct_writes_delta) "DirectWritesTotal",
    sum(SqlStat.disk_reads_delta) "DiskReadsTotal",
    sum(SqlStat.elapsed_time_delta) "ElapsedTimeTotal",
    sum(SqlStat.end_of_fetch_count_delta) "EndOfFetchCountTotal",
    sum(SqlStat.executions_delta) "ExecutionsTotal",
    sum(SqlStat.iowait_delta) "IoWaitTotal",
    sum(SqlStat.javexec_time_delta) "JavaExecTotal",
    sum(SqlStat.physical_read_bytes_delta) "PhysicalReadBytesTotal",
    sum(SqlStat.physical_write_bytes_delta) "PhysicalWriteBytesTotal",
    sum(SqlStat.plsexec_time_delta) "PlsExecTotal",
    sum(SqlStat.px_servers_execs_delta) "PxExecutionsTotal",
    sum(SqlStat.rows_processed_delta) "RowsTotal"
FROM dba_hist_sqlstat SqlStat
JOIN dba_hist_snapshot Snapshot
ON SqlStat.dbid = Snapshot.dbid
    AND SqlStat.instance_number = Snapshot.instance_number
    AND SqlStat.snap_id = Snapshot.snap_id
  -- use a query parameter to get the number of querylog days that should be loaded
    AND Snapshot.end_interval_time > sysdate - ?
GROUP BY
    SqlStat.dbid,
    SqlStat.instance_number,
    to_char(SqlStat.force_matching_signature)
