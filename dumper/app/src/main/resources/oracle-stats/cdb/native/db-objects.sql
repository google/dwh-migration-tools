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
  A.owner "Owner",
  A.object_name "ObjectName",
  A.object_type "ObjectType",
  A.editionable "Editionable",
  count(1) "Count"
FROM cdb_objects A
WHERE A.owner NOT LIKE '%SYS'
  AND A.object_name NOT LIKE 'BIN$%'
  AND (A.object_type <> 'SYNONYM' OR A.owner <> 'PUBLIC')
GROUP BY
  A.con_id,
  A.owner,
  A.object_name,
  A.object_type,
  A.editionable
UNION ALL
SELECT
  B.con_id "ConId",
  'SYS' "Owner",
  B.object_name "ObjectName",
  'DIRECTORY' "ObjectType",
  B.editionable "Editionable",
  count(1) "Count"
FROM cdb_objects B
WHERE B.owner = 'SYS'
  AND B.object_type = 'DIRECTORY'
  AND B.object_name NOT LIKE 'BIN$%'
GROUP BY
  B.con_id,
  B.owner,
  B.object_name,
  B.object_type,
  B.editionable
