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
SELECT con_id ,
       owner ,
       object_type ,
       editionable ,
       count
FROM (
        SELECT i.con_id,
               i.owner,
               i.object_type,
               i.editionable,
               COUNT(1)              count
        FROM
        (
        SELECT
               con_id,
               owner,
               object_type,
               editionable AS editionable,
               object_name
        FROM cdb_objects a
        WHERE  (owner = 'SYS' AND object_type = 'DIRECTORY')
           OR owner NOT LIKE '%SYS'
) i
        LEFT OUTER JOIN
        (
        SELECT 'SYNONYM' as object_type, owner, synonym_name, con_id, table_owner
        FROM cdb_synonyms b
        WHERE owner = 'PUBLIC') x
        ON i.object_type = x.object_type AND i.owner = x.owner AND i.object_name = x.synonym_name AND i.con_id = x.con_id
        WHERE (CASE WHEN i.object_type = 'SYNONYM' and i.owner ='PUBLIC' and ( i.object_name like '/%' OR x.table_owner IS NOT NULL) THEN 0 ELSE 1 END = 1)
        AND i.object_name NOT LIKE 'BIN$%'
        GROUP  BY  i.con_id, i.owner, i.editionable , i.object_type
)
