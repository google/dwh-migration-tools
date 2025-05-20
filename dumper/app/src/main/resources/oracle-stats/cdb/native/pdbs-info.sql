-- Copyright 2022-2025 Google LLC
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
  con_id "ConId",
  inst_id "InstId",
  sum(sum_bytes) "SumBytes",
  sum(used_mem) "UsedMem",
  sum(alloc_mem) "AllocMem",
  sum(max_mem) "MaxMem"
FROM (
  SELECT
    con_id,
    inst_id,
    sum(bytes) sum_bytes,
    NULL used_mem,
    NULL alloc_mem,
    NULL max_mem
  FROM gv$sgastat
  GROUP BY con_id, inst_id
  UNION (
    SELECT
      con_id,
      inst_id,
      NULL sum_bytes,
      sum(pga_used_mem) used_mem,
      sum(pga_alloc_mem) alloc_mem,
      sum(pga_max_mem) max_mem
    FROM gv$process
    GROUP BY con_id, inst_id
  )
)
GROUP BY con_id, inst_id
ORDER BY con_id, inst_id
