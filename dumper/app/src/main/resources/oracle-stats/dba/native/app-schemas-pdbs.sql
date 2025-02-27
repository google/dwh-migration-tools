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
  NULL "EbsOwner",
  NULL "SiebelOwner",
  NULL "PsftOwner",
  0 "RdaMatchCount",
  0 "OciAutoViewMatches",
  0 "DbmsCloudMatches",
  0 "ApexMatches",
  NULL "SapOwner"
FROM (
  SELECT
    'pdbs' source,
    A.dbid,
    A.pdb_id,
    A.pdb_name,
    A.status st,
    A.logging,
    A.con_id,
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
  INNER JOIN sys.obj$ C
  ON B.obj# = C.obj# AND B.con_id# = 1
) D
