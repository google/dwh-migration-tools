-- Copyright 2022-2024 Google LLC
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
  A.snap_id "SnapId",
  A.dbid "DbId",
  A.instance_number "InstanceNumber",
  A.old_hash_value "OldHashValue",
  A.command_type "CommandType",
  C.snap_time "SnapTime",
  A.executions "Executions",
  A.px_servers_executions "PxServersExecutions",
  A.elapsed_time "ElapsedTime",
  A.disk_reads "DiskReads",
  A.direct_writes "DirectWrites",
  A.end_of_fetch_count "EndOfFetchCount",
  A.rows_processed "RowsProcessed",
  A.buffer_gets "BufferGets",
  A.cpu_time "CpuTime",
  A.user_io_wait_time "UserIoWaitTime",
  A.cluster_wait_time "ClusterWaitTime",
  A.application_wait_time "ApplicationWaitTime",
  A.concurrency_wait_time "ConcurrencyWaitTime",
  A.plsql_exec_time "PlsqlExecTime",
  A.java_exec_time "JavaExecTime",
  to_char(C.snap_time, 'hh24') "Hh24",
  A.command_type "CommandType",
  D.name "CommandName",
  count(1) "Count"
FROM stats$SQL_SUMMARY A
INNER JOIN stats$snapshot C
  ON A.dbid = C.dbid
  AND A.snap_id = C.snap_id
  AND A.instance_number = C.instance_number
LEFT OUTER JOIN audit_actions D ON A.command_type = D.action
GROUP BY
  A.snap_id,
  A.dbid,
  A.instance_number,
  A.old_hash_value,
  A.command_type,
  C.snap_time,
  A.executions,
  A.px_servers_executions,
  A.elapsed_time,
  A.disk_reads,
  A.direct_writes,
  A.end_of_fetch_count,
  A.rows_processed,
  A.buffer_gets,
  A.cpu_time,
  A.user_io_wait_time,
  A.cluster_wait_time,
  A.application_wait_time,
  A.concurrency_wait_time,
  A.plsql_exec_time,
  A.java_exec_time,
  to_char(C.snap_time, 'hh24'),
  A.command_type,
  D.name
