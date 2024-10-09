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
  'PUBLIC' "Owner",
  'SYNONYM' "ObjectType",
  A.editionable "Editionable",
  A.object_name "ObjectName",
  -- "Count" is kept for backwards compatibility
  1 "Count",
  C.table_owner "TableOwner"
FROM cdb_objects A
LEFT OUTER JOIN cdb_synonyms C
  ON A.owner = C.owner
  AND C.table_owner IS NOT NULL
  AND A.object_name = C.synonym_name
  AND A.con_id = C.con_id
WHERE A.object_type = 'SYNONYM'
  AND A.object_name NOT LIKE '/%'
  AND A.object_name NOT LIKE 'BIN$%'
  AND A.owner = 'PUBLIC'
GROUP BY
  A.con_id,
  A.editionable,
  A.object_name,
  C.table_owner
