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
  Combined.inst_id "InstId",
  sum(Combined.bytes) "SumBytes",
  sum(Combined.pga_used_mem) "UsedMem",
  sum(Combined.pga_alloc_mem) "AllocMem",
  sum(Combined.pga_max_mem)"MaxMem"
FROM (
  SELECT
    Stat.inst_id,
    Stat.bytes,
    NULL pga_used_mem,
    NULL pga_alloc_mem,
    NULL pga_max_mem
  FROM gv$sgastat Stat
  UNION ALL
    SELECT
      Process.inst_id,
      NULL bytes,
      Process.pga_used_mem,
      Process.pga_alloc_mem,
      Process.pga_max_mem
    FROM gv$process Process
) Combined
GROUP BY inst_id
ORDER BY inst_id