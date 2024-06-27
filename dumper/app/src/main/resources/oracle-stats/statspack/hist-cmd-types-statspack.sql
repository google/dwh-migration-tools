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
  ss.snap_id "SnapId",
  ss.dbid "DbId",
  ss.instance_number "InstanceNumber",
  ss.old_hash_value "OldHashValue",
  ss.command_type "CommandType",
  C.snap_time "SnapTime",
  ss.executions "Executions",
  ss.px_servers_executions "PxServersExecutions",
  ss.elapsed_time "ElapsedTime",
  ss.disk_reads "DiskReads",
  ss.direct_writes "DirectWrites",
  ss.end_of_fetch_count "EndOfFetchCount",
  ss.rows_processed "RowsProcessed",
  ss.buffer_gets "BufferGets",
  ss.cpu_time "CpuTime",
  ss.user_io_wait_time "UserIoWaitTime",
  ss.cluster_wait_time "ClusterWaitTime",
  ss.application_wait_time "ApplicationWaitTime",
  ss.concurrency_wait_time "ConcurrencyWaitTime",
  ss.plsql_exec_time "PlsqlExecTime",
  ss.java_exec_time "JavaExecTime",
  to_char(C.snap_time, 'hh24') "Hh24",
  ss.command_type "CommandType",
  D.name "CommandName",
  count(1) "Count"
FROM
(
  SELECT
    snap_id,
    dbid,
    instance_number,
    text_subset,
    old_hash_value,
    command_type,
    force_matching_signature, sql_id,
    s.executions,
    px_servers_executions,
    elapsed_time,
    disk_reads,
    direct_writes,
    end_of_fetch_count,
    rows_processed,
    buffer_gets,
    cpu_time,
    user_io_wait_time,
    cluster_wait_time,
    application_wait_time,
    concurrency_wait_time,
    plsql_exec_time,
      java_exec_time
  FROM stats$SQL_SUMMARY s
) ss
JOIN stats$snapshot C
  ON ss.dbid = C.dbid
  AND ss.snap_id = C.snap_id
  AND ss.instance_number = C.instance_number
LEFT OUTER JOIN audit_actions D ON ss.command_type = D.action
GROUP BY
  ss.snap_id,
  ss.dbid,
  ss.instance_number,
  ss.old_hash_value,
  ss.command_type,
  C.snap_time,
  ss.executions,
  ss.px_servers_executions,
  ss.elapsed_time,
  ss.disk_reads,
  ss.direct_writes,
  ss.end_of_fetch_count,
  ss.rows_processed,
  ss.buffer_gets,
  ss.cpu_time,
  ss.user_io_wait_time,
  ss.cluster_wait_time,
  ss.application_wait_time,
  ss.concurrency_wait_time,
  ss.plsql_exec_time,
  ss.java_exec_time,
  to_char(C.snap_time, 'hh24'),
  ss.command_type,
  D.name
