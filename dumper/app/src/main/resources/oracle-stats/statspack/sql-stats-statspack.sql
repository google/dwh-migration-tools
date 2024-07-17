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
  A.instance_number "InstanceNumber",
  to_char(B.force_matching_signature) "ForceMatchingSignature",
  min(B.sql_id) "MinSqlId",
  sum(B.application_wait_time) "SumAPWait",
  sum(B.buffer_gets) "SumBufferGets",
  sum(B.concurrency_wait_time) "SumCCWait",
  sum(B.cluster_wait_time) "SumCLWait",
  sum(B.cpu_time) "SumCpuTime",
  sum(B.direct_writes) "SumDirectWrites",
  sum(B.disk_reads) "SumDiskReads",
  sum(B.elapsed_time) "SumElapsedTime",
  sum(B.end_of_fetch_count) "SumEndOfFetchCount",
  sum(B.executions) "SumExecutions",
  sum(B.user_io_wait_time) "SumIOWait",
  sum(B.java_exec_time) "SumJavaExec",
  sum(B.plsql_exec_time) "SumPlsExec",
  sum(B.px_servers_executions) "SumPxExecutions",
  sum(B.rows_processed) "SumRowsProcessed"
FROM
  stats$snapshot A
INNER JOIN stats$sql_summary B
  ON A.dbid = B.dbid
  AND A.snap_id = B.snap_id
  AND A.instance_number = B.instance_number
  AND A.snap_time > sysdate - ?
GROUP BY
  A.dbid,
  A.instance_number,
  B.force_matching_signature
