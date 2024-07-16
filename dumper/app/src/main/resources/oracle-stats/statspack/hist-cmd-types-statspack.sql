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
  B.name "CommandName",
  A.command_type "CommandType",
  A.dbid "DbId",
  to_char(C.snap_time, 'hh24') "Hour",
  A.instance_number "InstanceNumber",
  count(1) "Count",
  sum(A.application_wait_time) "SumAPWait",
  sum(A.buffer_gets) "SumBufferGets",
  sum(A.cpu_time) "SumCpuTime",
  sum(A.concurrency_wait_time) "SumCCWait",
  sum(A.cluster_wait_time) "SumCLWait",
  sum(A.direct_writes) "SumDirectWrites",
  sum(A.disk_reads) "SumDiskReads",
  sum(A.elapsed_time) "SumElapsedTime",
  sum(A.executions) "SumExecutions",
  sum(A.end_of_fetch_count) "SumFetchCount",
  sum(A.user_io_wait_time) "SumIOWait",
  sum(A.java_exec_time) "SumJavaExecTime",
  sum(A.plsql_exec_time) "SumPLSExecTime",
  sum(A.px_servers_executions) "SumPXExecutions",
  sum(A.rows_processed) "SumRowsProcessed"
FROM stats$SQL_SUMMARY A
LEFT JOIN audit_actions B ON A.command_type = B.action
INNER JOIN stats$snapshot C
  ON A.dbid = C.dbid
  AND A.snap_id = C.snap_id
  AND A.instance_number = C.instance_number
  AND C.snap_time > sysdate - ?
GROUP BY
  A.dbid,
  A.instance_number,
  A.old_hash_value,
  A.command_type,
  to_char(C.snap_time, 'hh24'),
  B.name
