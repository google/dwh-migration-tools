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
    A.con_id,
    TO_CHAR(C.begin_interval_time, 'HH24') hh,
    A.sql_id,
    A.dbid,
    B.command_type,
    count(1),
    sum(A.buffer_gets_delta) sum_buffer_gets_delta,
    sum(A.elapsed_time_delta) sum_elapsed_time_delta,
    sum(A.rows_processed_delta) rows_processed_delta,
    sum(A.executions_delta) sum_executions_delta,
    sum(A.cpu_time_delta) sum_cpu_time_delta,
    sum(A.apwait_delta) sum_apwait_delta,
    sum(A.ccwait_delta) sum_ccwait_delta,
    sum(A.plsexec_time_delta) sum_plsexec_time_delta
FROM '{prefix}'_hist_sqlstat A
JOIN '{prefix}'_hist_sqltext B
    ON A.con_id = B.con_id
    AND A.dbid = B.dbid
    AND A.sql_id = b.sql_id
JOIN '{prefix}'_hist_snapshot C
    ON A.dbid = C.dbid
    AND A.instance_number = C.instance_number
    AND A.snap_id = C.snap_id
GROUP BY
    A.con_id,
    TO_CHAR(C.begin_interval_time, 'HH24'),
    A.sql_id,
    A.dbid,
    B.command_type
