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

-- This deals with the simpler case of db-objects.
-- The other case is SYNONYM objects with PUBLIC owner, which require a JOIN
-- with a _synonyms table.
SELECT
  A.con_id "ConId",
  A.owner "Owner",
  A.object_type "ObjectType",
  A.editionable "Editionable",
  count(1) "Count"
FROM cdb_objects A
WHERE A.owner NOT LIKE '%SYS'
  AND A.object_name NOT LIKE 'BIN$%'
GROUP BY
  A.con_id,
  A.owner,
  A.object_type,
  A.editionable
UNION ALL (
  SELECT
    B.con_id "ConId",
    'SYS' "Owner",
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
    B.object_type,
    B.editionable
)
UNION ALL (
  SELECT
    C.con_id "ConId",
    C.owner "Owner",
    C.object_type "ObjectType",
    C.editionable "Editionable",
    count(1) "Count"
  FROM cdb_objects C
  WHERE C.owner NOT LIKE '%SYS'
    AND C.owner <> 'PUBLIC'
    AND C.object_type = 'SYNONYM'
    AND C.object_name NOT LIKE 'BIN$%'
  GROUP BY
    C.con_id,
    C.owner,
    C.editionable,
    C.object_type
)
