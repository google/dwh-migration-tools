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
    D.source "Source",
    D.dbid "DbId",
    D.pdb_id "PdbId",
    D.pdb_name "PdbName",
    D.st "Status",
    D.logging "Logging",
    D.con_id "ConId",
    D.con_uid "ConUid",
    F.min_owner "EbsOwner",
    H.min_owner "SiebelOwner",
    J.min_owner "PsftOwner",
    nvl(L.count, 0) "RdaMatchCount",
    nvl(N.count, 0) "OciAutoViewMatches",
    nvl(R.count, 0) "DbmsCloudMatches",
    nvl(U.count, 0) "ApexMatches",
    W.min_owner "SapOwner"
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
  SELECT E.con_id, min(E.owner) min_owner
  FROM cdb_tab_columns E
  WHERE E.table_name = 'FND_PRODUCT_GROUPS'
    AND E.column_name = 'RELEASE_NAME'
    AND E.data_type = 'VARCHAR2'
  GROUP BY E.con_id
) F ON D.con_id = F.con_id
LEFT JOIN (
  SELECT G.con_id, min(G.owner) min_owner
  FROM cdb_tab_columns G
  WHERE G.table_name = 'S_REPOSITORY'
    AND G.column_name = 'ROW_ID'
    AND G.data_type = 'VARCHAR2'
  GROUP BY G.con_id
) H ON D.con_id = H.con_id
LEFT JOIN (
  SELECT I.con_id, min(I.owner) min_owner
  FROM cdb_tab_columns I
  WHERE I.table_name = 'PSSTATUS'
    AND I.column_name = 'TOOLSREL'
    AND I.data_type = 'VARCHAR2'
  GROUP BY I.con_id
) J ON D.con_id = J.con_id
LEFT JOIN (
  SELECT K.con_id, count(1) count FROM cdb_objects K
  WHERE K.owner = 'RDSADMIN'
    AND K.object_name = 'RDAADMIN_UTIL'
  GROUP BY K.con_id
) L ON D.con_id = L.con_id
LEFT JOIN (
  SELECT M.con_id, count(1) count FROM cdb_views M
  WHERE view_name = 'OCI_AUTONOMOUS_DATABASES'
  GROUP BY M.con_id
) N ON D.con_id = N.con_id
LEFT JOIN (
  SELECT P.con_id, count(1) count FROM cdb_objects P
  JOIN v$parameter Q
    ON P.object_name = 'DBMS_CLOUD'
    AND Q.name = 'common_user_prefix'
    AND P.con_id = Q.con_id
    AND P.owner = 'CLOUD$SERVICE' || Q.value
  GROUP BY P.con_id
) R ON D.con_id = R.con_id
LEFT JOIN (
  SELECT S.con_id, count(1) count FROM cdb_objects S
  JOIN cdb_users T
    ON S.object_name = 'WWV_FLOW'
    AND S.object_type = 'PACKAGE'
    AND T.username = 'apex_public_user'
    AND S.con_id = T.con_id
  GROUP BY S.con_id
) U ON D.con_id = U.con_id
LEFT JOIN (
  SELECT V.con_id, min(V.owner) min_owner FROM cdb_tab_columns V
  WHERE V.table_name = 'DD02T'
    AND V.column_name = 'DDLANGUAGE'
    AND V.data_type = 'VARCHAR2'
  GROUP BY V.con_id, V.owner
) W ON D.con_id = W.con_id
