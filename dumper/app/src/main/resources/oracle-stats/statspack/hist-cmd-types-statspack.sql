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
  A.px_servers_executions "PxServersExecutions",
  A.disk_reads "DiskReads",
  A.direct_writes "DirectWrites",
  A.end_of_fetch_count "EndOfFetchCount",
  A.user_io_wait_time "UserIoWaitTime",
  A.java_exec_time "JavaExecTime",
  to_char(C.snap_time, 'hh24') "Hh24",
  A.command_type "CommandType",
  D.name "CommandName",
  count(1) "Count",
  sum(A.application_wait_time) "SumAPWait",
  sum(A.buffer_gets) "SumBufferGets",
  sum(A.cpu_time) "SumCpuTime",
  sum(A.concurrency_wait_time) "SumCCWait",
  sum(A.cluster_wait_time) "SumCLWait",
  sum(A.elapsed_time) "SumElapsedTime",
  sum(A.executions) "SumExecutions",
  sum(A.user_io_wait_time) "SumIOWait",
  sum(A.plsql_exec_time) "SumPLSExecTime",
  sum(A.rows_processed) "SumRowsProcessed"
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
  A.px_servers_executions,
  A.disk_reads,
  A.direct_writes,
  A.end_of_fetch_count,
  A.user_io_wait_time,
  A.java_exec_time,
  to_char(C.snap_time, 'hh24'),
  A.command_type,
  D.name
