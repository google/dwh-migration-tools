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
  ActiveSession.con_id "ConId",
  ActiveSession.dbid "DbId",
  to_char(AshSnap.begin_interval_time, 'hh24') "Hour",
  ActiveSession.instance_number "InstanceNumber",
  ActiveSession.program "Program",
  ActiveSession.module "Module",
  ActiveSession.machine "Machine",
  ActiveSession.sql_id "SqlId",
  C.command_name "CommandName",
  COUNT(*) "Count",
  SUM(ActiveSession.delta_read_io_bytes) "ReadIoBytesTotal",
  SUM(ActiveSession.delta_write_io_bytes) "WriteIoBytesTotal",
  SUM(ActiveSession.delta_read_io_requests) "ReadIoRequestsTotal",
  SUM(ActiveSession.delta_write_io_requests) "WriteIoRequestsTotal",
  SUM(ActiveSession.tm_delta_cpu_time) "CpuTimeTotal"
FROM cdb_hist_active_sess_history ActiveSession
INNER JOIN cdb_hist_ash_snapshot AshSnap
  ON ActiveSession.snap_id = AshSnap.snap_id
  AND ActiveSession.instance_number = AshSnap.instance_number
  AND ActiveSession.dbid = AshSnap.dbid
  AND ActiveSession.session_type = 'FOREGROUND'
  -- use a query parameter to get the number of querylog days that should be loaded
  AND AshSnap.end_interval_time > sysdate - ?
INNER JOIN v$sqlcommand C
  ON ActiveSession.sql_opcode = C.command_type
GROUP BY
  ActiveSession.con_id,
  ActiveSession.dbid,
  ActiveSession.instance_number,
  ActiveSession.program,
  ActiveSession.module,
  ActiveSession.machine,
  to_char(AshSnap.begin_interval_time, 'hh24'),
  C.command_name,
  ActiveSession.sql_id
