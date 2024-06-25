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
    sum(A.executions_delta) "SumExecutionsDelta",
    sum(A.px_servers_execs_delta) "SumPxServersExecsDelta",
    sum(A.elapsed_time_total) "SumElapsedTimeTotal",
    sum(A.disk_reads_delta) "SumDiskReadsDelta",
    sum(A.physical_read_bytes_delta) "SumPhysicalReadBytesDelta",
    sum(A.physical_write_bytes_delta) "SumPhysicalWriteBytesDelta",
    sum(A.io_offload_elig_bytes_delta) "SumIoOffloadEligBytesDelta",
    sum(A.io_interconnect_bytes_delta) "SumIoInterconnectBytesDelta",
    sum(A.optimized_physical_reads_delta) "SumOptimizedPhysicalReadsDelta",
    sum(A.cell_uncompressed_bytes_delta) "SumCellUncompressedBytesDelta",
    sum(A.io_offload_return_bytes_delta) "SumIoOffloadReturnBytesDelta",
    sum(A.direct_writes_delta) "SumDirectWritesDelta",
    sum(A.end_of_fetch_count_delta) "SumEndOfFetchCountDelta",
    sum(A.rows_processed_delta) "SumRowsProcessedDelta",
    sum(A.buffer_gets_delta) "SumBufferGetsDelta",
    sum(A.cpu_time_delta) "SumCpuTimeDelta",
    sum(A.elapsed_time_delta) "SumElapsedTimeDelta",
    sum(A.iowait_delta) "SumIowaitDelta",
    sum(A.clwait_delta) "SumClwaitDelta",
    sum(A.apwait_delta) "SumApwaitDelta",
    sum(A.ccwait_delta) "SumCcwaitDelta",
    sum(A.plsexec_time_delta) "SumPlsexecTimeDelta",
    sum(A.javexec_time_delta) "SumJavexecTimeDelta"
FROM cdb_hist_sqlstat A
JOIN (
    SELECT B.snap_id, B.instance_number, B.dbid
    FROM cdb_hist_snapshot B
) B
ON A.dbid = B.dbid
    AND A.instance_number = B.instance_number
    AND A.snap_id = B.snap_id
GROUP BY
    A.con_id,
    A.dbid,
    A.instance_number,
    to_char(A.force_matching_signature)
