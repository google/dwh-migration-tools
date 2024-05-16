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
    D.source "Source",
    D.dbid "DbId",
    D.pdb_id "PdbId",
    D.pdb_name "PdbName",
    D.st "Status",
    D.logging "Logging",
    D.con_id "ConId",
    D.con_uid "ConUid",
    E.min_owner "Owner"
FROM (
  SELECT
    'pdbs' source,
    A.dbid,
    A.pdb_id,
    A.pdb_name pdb_name,
    A.status st,
    A.logging,
    A.con_id con_id,
    A.con_uid
  FROM cdb_pdbs A
  UNION
  SELECT
    'sys' source,
    B.dbid,
    B.con_id# pdb_id,
    C.name pdb_name,
    to_char(B.status) st,
    to_char(B.flags) logging,
    B.con_id# con_id,
    B.con_uid
  FROM sys.container$ B
  JOIN sys.obj$ C
  ON B.obj# = C.obj# AND B.con_id# = 1
) D
LEFT JOIN (
  SELECT F.con_id, min(F.owner) min_owner
  FROM cdb_tab_columns F
  WHERE F.table_name = 'FND_PRODUCT_GROUPS'
    AND F.column_name = 'RELEASE_NAME'
    AND F.data_type = 'VARCHAR2'
  GROUP BY F.con_id
) E ON D.con_id = E.con_id
