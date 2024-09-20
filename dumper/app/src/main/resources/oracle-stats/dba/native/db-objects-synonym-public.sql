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
  'PUBLIC' "Owner",
  'SYNONYM' "ObjectType",
  A.editionable "Editionable",
  A.object_name "ObjectName",
  -- "Count" is kept for backwards compatibility
  CASE WHEN A.object_name LIKE '/%' THEN 0
    WHEN A.object_name LIKE 'BIN$%' THEN 0
    ELSE 1 END "Count"
FROM dba_objects A
  WHERE A.object_type = 'SYNONYM'
    AND A.owner = 'PUBLIC'
GROUP BY
  A.editionable,
  A.object_name
