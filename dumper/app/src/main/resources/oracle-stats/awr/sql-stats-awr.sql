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
    A.dbid "Dbid",
    A.instance_number "InstanceNumber",
    to_char(A.force_matching_signature) "ForceMatchingSignature",
    min(A.sql_id) "MinSqlId",
    min(B.begin_interval_time) "MinBeginTime",
    max(B.end_interval_time) "MaxEndTime",
    sum(A.apwait_total) "SumAPWait",
    sum(A.buffer_gets_total) "SumBufferGets",
    sum(A.ccwait_total) "SumCCWait",
    sum(A.clwait_total) "SumCLWait",
    sum(A.cpu_time_total) "SumCpuTime",
    sum(A.direct_writes_total) "SumDirectWrites",
    sum(A.disk_reads_total) "SumDiskReads",
    sum(A.elapsed_time_total) "SumElapsedTime",
    sum(A.end_of_fetch_count_total) "SumEndOfFetchCount",
    sum(A.executions_total) "SumExecutions",
    sum(A.iowait_total) "SumIOWait",
    sum(A.javexec_time_total) "SumJavaExec",
    sum(A.plsexec_time_total) "SumPlsExec",
    sum(A.px_servers_execs_total) "SumPxExecutions",
    sum(A.rows_processed_total) "SumRowsProcessed"
FROM cdb_hist_sqlstat A
JOIN cdb_hist_snapshot B
ON A.dbid = B.dbid
    AND A.instance_number = B.instance_number
    AND A.snap_id = B.snap_id
GROUP BY
    A.con_id,
    A.dbid,
    A.instance_number,
    to_char(A.force_matching_signature)
