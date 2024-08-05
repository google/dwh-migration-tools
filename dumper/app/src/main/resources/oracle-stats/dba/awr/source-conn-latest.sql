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
  NULL "ConId",
  History.dbid "DbId",
  to_char(Snapshot.begin_interval_time, 'hh24') "Hour",
  History.instance_number "InstanceNumber",
  History.program "Program",
  History.module "Module",
  History.machine "Machine",
  Command.command_name "CommandName",
  count(1) "Count"
FROM dba_hist_active_sess_history History
INNER JOIN dba_hist_snapshot Snapshot
  ON History.snap_id = Snapshot.snap_id
  AND History.instance_number = Snapshot.instance_number
  AND History.dbid = Snapshot.dbid
  AND History.session_type = 'FOREGROUND'
  -- use a query parameter to get the number of querylog days that should be loaded
  AND Snapshot.end_interval_time > sysdate - ?
INNER JOIN v$sqlcommand Command
  ON History.sql_opcode = Command.command_type
GROUP BY
  History.dbid,
  History.instance_number,
  History.program,
  History.module,
  History.machine,
  to_char(Snapshot.begin_interval_time, 'hh24'),
  Command.command_name
