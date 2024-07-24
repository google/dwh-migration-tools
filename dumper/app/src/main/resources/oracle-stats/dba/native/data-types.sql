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
  A.owner "Owner",
  A.data_type "DataType",
  A.data_length "DataLength",
  A.data_precision "DataPrecision",
  A.data_scale "DataScale",
  A.avg_col_len "AvgColLen",
  count(1) "Count",
  count(DISTINCT table_name) "DistinctTableCount"
FROM dba_tab_columns A
INNER JOIN dba_objects B
  ON A.owner = B.owner
  AND A.owner NOT LIKE '%SYS'
  AND A.table_name = B.object_name
  AND B.object_type = 'TABLE'
GROUP BY
  A.owner,
  A.data_type,
  A.data_length,
  A.data_precision,
  A.data_scale,
  A.avg_col_len
