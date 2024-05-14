-- Copyright 2022 Google LLC
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
  ss.snap_id,
  ss.dbid,
  ss.instance_number,
  ss.text_subset,
  ss.old_hash_value,
  ss.command_type,
  ss.force_matching_signature,
  ss.sql_id,
  C.snap_time,
  ss.executions,
  ss.delta_executions,
  ss.px_servers_executions,
  ss.delta_px_servers_executions,
  ss.elapsed_time,
  ss.delta_elapsed_time,
  ss.disk_reads,
  ss.delta_disk_reads,
  ss.direct_writes,
  ss.delta_direct_writes,
  ss.end_of_fetch_count,
  ss.delta_end_of_fetch_count,
  ss.rows_processed,
  ss.delta_rows_processed,
  ss.buffer_gets,
  ss.delta_buffer_gets,
  ss.cpu_time,
  ss.delta_cpu_time,
  ss.user_io_wait_time,
  ss.delta_user_io_wait_time,
  ss.cluster_wait_time,
  ss.delta_cluster_wait_time,
  ss.application_wait_time,
  ss.delta_application_wait_time,
  ss.concurrency_wait_time,
  ss.delta_concurrency_wait_time,
  ss.plsql_exec_time,
  ss.delta_plsql_exec_time,
  ss.java_exec_time,
  ss.delta_java_exec_time,
  'N/A' con_id,
  TO_CHAR(C.snap_time, 'hh24') hh24,
  ss.command_type,
  D.name command_name
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
    NVL(
      DECODE(
        GREATEST(
          executions,
          NVL(
            LAG(executions) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        executions,
        executions - LAG(executions) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_executions,
    px_servers_executions,
    NVL(
      DECODE(
        GREATEST(
          px_servers_executions,
          NVL(
            LAG(px_servers_executions) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        px_servers_executions,
        px_servers_executions - LAG(px_servers_executions) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_px_servers_executions,
    elapsed_time,
    NVL(
      DECODE(
        GREATEST(
          elapsed_time,
          NVL(
            LAG(elapsed_time) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        elapsed_time,
        elapsed_time - LAG(elapsed_time) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_elapsed_time,
    disk_reads,
    NVL(
      DECODE(
        GREATEST(
          disk_reads,
          NVL(
            LAG(disk_reads) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        disk_reads,
        disk_reads - LAG(disk_reads) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_disk_reads,
    direct_writes,
    NVL(
      DECODE(
        GREATEST(
          direct_writes,
          NVL(
            LAG(direct_writes) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        direct_writes,
        direct_writes - LAG(direct_writes) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_direct_writes,
    end_of_fetch_count,
    NVL(
      DECODE(
        GREATEST(
          end_of_fetch_count,
          NVL(
            LAG(end_of_fetch_count) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        end_of_fetch_count,
        end_of_fetch_count - LAG(end_of_fetch_count) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_end_of_fetch_count,
    rows_processed,
    NVL(
      DECODE(
        GREATEST(
          rows_processed,
          NVL(
            LAG(rows_processed) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        rows_processed,
        rows_processed - LAG(rows_processed) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_rows_processed,
    buffer_gets,
    NVL(
      DECODE(
        GREATEST(
          buffer_gets,
          NVL(
            LAG(buffer_gets) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        buffer_gets,
        buffer_gets - LAG(buffer_gets) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_buffer_gets,
    cpu_time,
    NVL(
      DECODE(
        GREATEST(
          cpu_time,
          NVL(
            LAG(cpu_time) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        cpu_time,
        cpu_time - LAG(cpu_time) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_cpu_time,
    user_io_wait_time,
    NVL(
      DECODE(
        GREATEST(
          user_io_wait_time,
          NVL(
            LAG(user_io_wait_time) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        user_io_wait_time,
        user_io_wait_time - LAG(user_io_wait_time) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_user_io_wait_time,
    cluster_wait_time,
    NVL(
      DECODE(
        GREATEST(
          cluster_wait_time,
          NVL(
            LAG(cluster_wait_time) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        cluster_wait_time,
        cluster_wait_time - LAG(cluster_wait_time) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_cluster_wait_time,
    application_wait_time,
    NVL(
      DECODE(
        GREATEST(
          application_wait_time,
          NVL(
            LAG(application_wait_time) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        application_wait_time,
        application_wait_time - LAG(application_wait_time) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_application_wait_time,
    concurrency_wait_time,
    NVL(
      DECODE(
        GREATEST(
          concurrency_wait_time,
          NVL(
            LAG(concurrency_wait_time) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        concurrency_wait_time,
        concurrency_wait_time - LAG(concurrency_wait_time) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_concurrency_wait_time,
    plsql_exec_time,
      NVL(
        DECODE(
          GREATEST(
            plsql_exec_time,
            NVL(
              LAG(plsql_exec_time) OVER (
                PARTITION BY
                  s.dbid,
                  s.instance_number,
                  text_subset,
                  old_hash_value,
                  command_type,
                  force_matching_signature
                ORDER BY s.snap_id
              ),
              0
            )
          ),
          plsql_exec_time,
          plsql_exec_time - LAG(plsql_exec_time) OVER (
            PARTITION BY
              s.dbid,
              s.instance_number,
              text_subset,
              old_hash_value,
              command_type,
              force_matching_signature
            ORDER BY s.snap_id
          ),
          0
        ),
        0
      ) delta_plsql_exec_time,
      java_exec_time,
        NVL(
        DECODE(
          GREATEST(java_exec_time, NVL(
            LAG(java_exec_time) OVER (
              PARTITION BY
                s.dbid,
                s.instance_number,
                text_subset,
                old_hash_value,
                command_type,
                force_matching_signature
              ORDER BY s.snap_id
            ),
            0
          )
        ),
        java_exec_time,
        java_exec_time - LAG(java_exec_time) OVER (
          PARTITION BY
            s.dbid,
            s.instance_number,
            text_subset,
            old_hash_value,
            command_type,
            force_matching_signature
          ORDER BY s.snap_id
        ),
        0
      ),
      0
    ) delta_java_exec_time
  FROM stats$SQL_SUMMARY s
) ss
JOIN stats$snapshot C
  ON ss.dbid = C.dbid
  AND ss.snap_id = C.snap_id
  AND ss.instance_number = C.instance_number
LEFT OUTER JOIN audit_actions D ON ss.command_type = D.action
