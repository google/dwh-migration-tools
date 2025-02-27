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
  SqlText.command_type "CommandType",
  to_char(Snapshot.begin_interval_time, 'hh24') "Hour",
  Action.name "CommandName",
  count(1) "Count",
  avg(SqlStat.buffer_gets_delta) "AvgBufferGets",
  avg(SqlStat.elapsed_time_delta) "AvgElapsedTime",
  avg(SqlStat.rows_processed_delta) "AvgRowsProcessed",
  avg(SqlStat.executions_delta) "AvgExecutions",
  avg(SqlStat.cpu_time_delta) "AvgCpuTime",
  avg(SqlStat.iowait_delta) "AvgIoWait",
  avg(SqlStat.clwait_delta) "AvgClWait",
  avg(SqlStat.apwait_delta) "AvgApWait",
  avg(SqlStat.ccwait_delta) "AvgCcWait",
  avg(SqlStat.plsexec_time_delta) "AvgPlsExecTime",
  avg(SqlStat.physical_write_bytes_delta) "AvgPhysicalWriteBytes",
  avg(SqlStat.physical_write_requests_delta) "AvgPhysicalWriteRequests",
  avg(SqlStat.direct_writes_delta) "AvgDirectWrites",
  avg(SqlStat.disk_reads_delta) "AvgDiskReads",
  avg(SqlStat.physical_read_requests_delta) "AvgPhysicalReadRequests",
  avg(SqlStat.physical_read_bytes_delta) "AvgPhysicalReadBytes"
FROM dba_hist_sqlstat SqlStat
INNER JOIN dba_hist_sqltext SqlText
  ON SqlStat.sql_id = SqlText.sql_id
  AND SqlStat.dbid = SqlText.dbid
INNER JOIN dba_hist_snapshot Snapshot
  ON SqlStat.snap_id = Snapshot.snap_id
  AND SqlStat.dbid = Snapshot.dbid
  AND SqlStat.instance_number = Snapshot.instance_number
  -- use a query parameter to get the number of querylog days that should be loaded
  AND Snapshot.end_interval_time > sysdate - ?
LEFT JOIN audit_actions Action
  ON SqlText.command_type = Action.action
GROUP BY
  SqlText.command_type,
  to_char(Snapshot.begin_interval_time, 'hh24'),
  Action.name
