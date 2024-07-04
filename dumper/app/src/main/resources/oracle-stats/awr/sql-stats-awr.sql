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
    min(A.sql_id) "SqlId",
    min(B.begin_interval_time) "MinBeginTime",
    max(B.end_interval_time) "MaxEndTime",
    sum(A.apwait_delta) "ApWaitTotal",
    sum(A.buffer_gets_delta) "BufferGetsTotal",
    sum(A.ccwait_delta) "CcWaitTotal",
    sum(A.clwait_delta) "ClWaitTotal",
    sum(A.cpu_time_delta) "CpuTimeTotal",
    sum(A.direct_writes_delta) "DirectWritesTotal",
    sum(A.disk_reads_delta) "DiskReadsTotal",
    sum(A.elapsed_time_delta) "ElapsedTimeTotal",
    sum(A.end_of_fetch_count_delta) "EndOfFetchCountTotal",
    sum(A.executions_delta) "ExecutionsTotal",
    sum(A.iowait_delta) "IoWaitTotal",
    sum(A.javexec_time_delta) "JavaExecTotal",
    sum(A.physical_read_bytes_delta) "PhysicalReadBytesTotal",
    sum(A.physical_write_bytes_delta) "PhysicalWriteBytesTotal",
    sum(A.plsexec_time_delta) "PlsExecTotal",
    sum(A.px_servers_execs_delta) "PxExecutionsTotal",
    sum(A.rows_processed_delta) "RowsTotal"
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
