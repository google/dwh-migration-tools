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
  A.con_id "ConId",
  B.command_type "CommandType",
  to_char(C.begin_interval_time, 'hh24') "Hour",
  D.name "CommandName",
  count(1) "Count",
  sum(A.buffer_gets_total) "SumBufferGets",
  sum(A.elapsed_time_total) "SumElapsedTime",
  sum(A.rows_processed_total) "SumRowsProcessed",
  sum(A.executions_total) "SumExecutions",
  sum(A.cpu_time_total) "SumCpuTime",
  sum(A.iowait_total) "SumIOWait",
  sum(A.clwait_total) "SumCLWait",
  sum(A.apwait_total) "SumAPWait",
  sum(A.ccwait_total) "SumCCWait",
  sum(A.plsexec_time_total) "SumPLSExecTime"
FROM cdb_hist_sqlstat A
INNER JOIN cdb_hist_sqltext B
  ON A.con_id = B.con_id
  AND A.sql_id = B.sql_id
  AND A.dbid = B.dbid
INNER JOIN cdb_hist_snapshot C
  ON A.snap_id = C.snap_id
  AND A.dbid = C.dbid
  AND A.instance_number = C.instance_number
LEFT JOIN audit_actions D
  ON B.command_type = D.action
GROUP BY
  A.con_id,
  B.command_type,
  to_char(C.begin_interval_time, 'hh24'),
  D.name
