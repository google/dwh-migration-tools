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
  (
    SELECT min(E.owner) "EbsOwner"
    FROM cdb_tab_columns E
    WHERE E.table_name = 'FND_PRODUCT_GROUPS'
      AND E.column_name = 'RELEASE_NAME'
      AND E.data_type = 'VARCHAR2'
  ) "EbsOwner",
  (
    SELECT min(G.owner) FROM cdb_tab_columns G
    WHERE G.table_name = 'S_REPOSITORY'
      AND G.column_name = 'ROW_ID'
      AND G.data_type = 'VARCHAR2'
  ) "SiebelOwner",
  (
    SELECT min(I.owner) FROM cdb_tab_columns I
    WHERE I.table_name = 'PSSTATUS'
      AND I.column_name = 'TOOLSREL'
      AND I.data_type = 'VARCHAR2'
  ) "PsftOwner",
  (
    SELECT count(1) FROM cdb_objects K
    WHERE K.owner = 'RDSADMIN'
      AND K.object_name = 'RDAADMIN_UTIL'
  ) "RdaMatchCount",
  (
    SELECT count(1) FROM cdb_views M
    WHERE view_name = 'OCI_AUTONOMOUS_DATABASES'
  ) "OciAutoViewMatches",
  (
    SELECT count(1) FROM cdb_objects P
    INNER JOIN v$parameter Q
      ON P.object_name = 'DBMS_CLOUD'
      AND Q.name = 'common_user_prefix'
      AND P.owner = 'CLOUD$SERVICE' || Q.value
  ) "DbmsCloudMatches",
  (
    SELECT count(1) FROM cdb_objects S
    INNER JOIN cdb_users T
      ON S.object_name = 'WWV_FLOW'
      AND S.object_type = 'PACKAGE'
      AND T.username = 'apex_public_user'
  ) "ApexMatches",
  (
    SELECT min(V.owner) FROM cdb_tab_columns V
    WHERE V.table_name = 'DD02T'
      AND V.column_name = 'DDLANGUAGE'
      AND V.data_type = 'VARCHAR2'
  ) "SapOwner"
FROM dual
