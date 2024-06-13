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
  B.con_id,
  B.owner,
  B.object_type,
  B.editionable,
  COUNT(1) count
FROM (
  SELECT
    A.con_id,
    A.owner,
    A.object_type,
    A.editionable,
    A.object_name
  FROM cdb_objects A
  WHERE  (A.owner = 'SYS' AND A.object_type = 'DIRECTORY')
    OR A.owner NOT LIKE '%SYS'
) B
LEFT OUTER JOIN (
  SELECT
    C.owner, synonym_name,
    C.con_id,
    C.table_owner
  FROM cdb_synonyms C
  WHERE C.owner = 'PUBLIC'
) D ON B.object_type = 'SYNONYM'
  AND B.owner = D.owner
  AND B.object_name = D.synonym_name
  AND B.con_id = D.con_id
WHERE (
    B.object_type <> 'SYNONYM'
    OR B.owner <> 'PUBLIC'
    OR (B.object_name NOT LIKE '/%' AND D.table_owner IS NULL)
  )
  AND B.object_name NOT LIKE 'BIN$%'
GROUP  BY  B.con_id, B.owner, B.editionable , B.object_type
