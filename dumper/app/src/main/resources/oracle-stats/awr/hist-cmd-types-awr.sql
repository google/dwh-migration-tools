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
  avg(A.buffer_gets_delta) "AvgBufferGets",
  avg(A.elapsed_time_delta) "AvgElapsedTime",
  avg(A.rows_processed_delta) "AvgRowsProcessed",
  avg(A.executions_delta) "AvgExecutions",
  avg(A.cpu_time_delta) "AvgCpuTime",
  avg(A.iowait_delta) "AvgIoWait",
  avg(A.clwait_delta) "AvgClWait",
  avg(A.apwait_delta) "AvgApWait",
  avg(A.ccwait_delta) "AvgCcWait",
  avg(A.plsexec_time_delta) "AvgPlsExecTime"
FROM cdb_hist_sqlstat A
INNER JOIN cdb_hist_sqltext B
  ON A.con_id = B.con_id
  AND A.sql_id = B.sql_id
  AND A.dbid = B.dbid
INNER JOIN cdb_hist_snapshot C
  ON A.snap_id = C.snap_id
  AND A.dbid = C.dbid
  AND A.instance_number = C.instance_number
  AND C.end_interval_time > sysdate - 30
LEFT JOIN audit_actions D
  ON B.command_type = D.action
GROUP BY
  A.con_id,
  B.command_type,
  to_char(C.begin_interval_time, 'hh24'),
  D.name
