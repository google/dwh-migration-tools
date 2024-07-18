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
    (
      SELECT min(E.owner) min_owner FROM dba_tab_columns E
      WHERE E.table_name = 'FND_PRODUCT_GROUPS'
        AND E.column_name = 'RELEASE_NAME'
        AND E.data_type = 'VARCHAR2'
    ) "EbsOwner",
    (
      SELECT min(G.owner) FROM dba_tab_columns G
      WHERE G.table_name = 'S_REPOSITORY'
        AND G.column_name = 'ROW_ID'
        AND G.data_type = 'VARCHAR2'
    ) "SiebelOwner",
    (
      SELECT min(I.owner) FROM dba_tab_columns I
      WHERE I.table_name = 'PSSTATUS'
        AND I.column_name = 'TOOLSREL'
        AND I.data_type = 'VARCHAR2'
    ) "PsftOwner",
    (
      SELECT count(1) FROM dba_objects K
      WHERE K.owner = 'RDSADMIN'
        AND K.object_name = 'RDAADMIN_UTIL'
    ) "RdaMatchCount",
    (
      SELECT count(1) FROM dba_views M
      WHERE view_name = 'OCI_AUTONOMOUS_DATABASES'
    ) "OciAutoViewMatches",
    nvl(R.count, 0) "DbmsCloudMatches",
    (
      SELECT count(1) FROM dba_objects S
      JOIN dba_users T
        ON S.object_name = 'WWV_FLOW'
        AND S.object_type = 'PACKAGE'
        AND T.username = 'apex_public_user'
    ) "ApexMatches",
    (
      SELECT min(V.owner) FROM dba_tab_columns V
      WHERE V.table_name = 'DD02T'
        AND V.column_name = 'DDLANGUAGE'
        AND V.data_type = 'VARCHAR2'
    ) "SapOwner"
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
  FROM dba_pdbs A
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
  SELECT Q.con_id, count(1) count FROM dba_objects P
  JOIN v$parameter Q
    ON P.object_name = 'DBMS_CLOUD'
    AND Q.name = 'common_user_prefix'
    AND P.owner = 'CLOUD$SERVICE' || Q.value
  GROUP BY Q.con_id
) R ON D.con_id = R.con_id
