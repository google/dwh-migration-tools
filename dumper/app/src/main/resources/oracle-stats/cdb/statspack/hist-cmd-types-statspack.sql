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
  A.command_type "CommandType",
  to_char(C.snap_time, 'hh24') "Hour",
  B.name "CommandName",
  count(1) "Count",
  avg(A.buffer_gets) "AvgBufferGets",
  avg(A.elapsed_time) "AvgElapsedTime",
  avg(A.rows_processed) "AvgRowsProcessed",
  avg(A.executions) "AvgExecutions",
  avg(A.cpu_time) "AvgCpuTime",
  avg(A.user_io_wait_time) "AvgIoWait",
  avg(A.cluster_wait_time) "AvgClWait",
  avg(A.application_wait_time) "AvgApWait",
  avg(A.concurrency_wait_time) "AvgCcWait",
  avg(A.plsql_exec_time) "AvgPlsExecTime"
FROM stats$sql_summary A
LEFT JOIN audit_actions B ON A.command_type = B.action
INNER JOIN stats$snapshot C
  ON A.dbid = C.dbid
  AND A.snap_id = C.snap_id
  AND A.instance_number = C.instance_number
  AND C.snap_time > sysdate - ?
GROUP BY
  A.command_type,
  to_char(C.snap_time, 'hh24'),
  B.name
