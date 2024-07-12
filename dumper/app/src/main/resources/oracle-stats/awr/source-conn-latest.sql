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
  A.dbid "DbId",
  to_char(B.begin_interval_time, 'hh24') "Hour",
  A.instance_number "InstanceNumber",
  A.program "Program",
  A.module "Module",
  A.machine "Machine",
  C.command_name "CommandName",
  count(1) "Count"
FROM cdb_hist_active_sess_history A
INNER JOIN cdb_hist_snapshot B
  ON A.snap_id = B.snap_id
  AND A.instance_number = B.instance_number
  AND A.dbid = B.dbid
  AND A.session_type = 'FOREGROUND'
  AND B.end_interval_time > sysdate - 30
INNER JOIN v$sqlcommand C
  ON A.sql_opcode = C.command_type
GROUP BY
  A.dbid,
  A.instance_number,
  A.program,
  A.module,
  A.machine,
  to_char(B.begin_interval_time, 'hh24'),
  C.command_name
