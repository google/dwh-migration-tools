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
  Cell.con_id "ConId",
  Cell.inst_id "InstId",
  Cell.cell_path "CellPath",
  Cell.cell_type "CellType",
  CellConfig.conftype "ConfigType",
  CellConfig.confval "ConfigValue"
FROM gv$cell Cell
LEFT JOIN gv$cell_config CellConfig
  ON Cell.con_id = CellConfig.con_id
  AND Cell.cell_hashval = CellConfig.cellhash
