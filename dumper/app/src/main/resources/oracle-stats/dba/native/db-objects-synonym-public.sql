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
  B.editionable "Editionable",
  B.object_name "ObjectName",
  -- This looks similar to filtering with WHERE and using count() instead of sum().
  --
  -- It is not similar. DB will see the LIKE inside a WHERE predicate and decide to
  -- replace a HASH JOIN with NESTED LOOPS. The JOIN arguments have >10k rows each,
  -- so performance-wise the nested loop would be terrible.
  sum(
    CASE WHEN B.object_name LIKE '/%' THEN 0
    WHEN B.object_name LIKE 'BIN$%' THEN 0
    ELSE 1 END
  ) "Count"
FROM (
  SELECT
    A.editionable,
    A.object_name,
    A.owner
  FROM dba_objects A
  WHERE A.object_type = 'SYNONYM'
    AND A.owner = 'PUBLIC'
) B
LEFT JOIN (
  SELECT
    C.synonym_name,
    C.table_owner
  FROM dba_synonyms C
  WHERE C.owner = 'PUBLIC'
    AND C.table_owner IS NOT NULL
) D ON B.object_name = D.synonym_name
WHERE D.table_owner IS NULL
    AND B.owner = 'PUBLIC'
GROUP BY
  B.editionable,
  B.object_name
