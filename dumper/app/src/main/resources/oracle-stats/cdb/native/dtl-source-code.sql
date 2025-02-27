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
  B.con_id "ConId",
  B.owner "Owner",
  B.name "Name",
  B.type "Type",
  count(1) "Objects",
  max(B.line) "Lines",
  sum(B.dbms) "LinesDbms",
  sum(B.dbms_sql) "LinesDbmsSql",
  sum(B.exec_im) "LinesExecIm",
  sum(B.utl) "LinesUtl",
  sum(least(B.dbms, B.utl)) "LinesDbmsAndUtl"
FROM (
  SELECT
    A.con_id,
    A.owner,
    A.name,
    A.type,
    A.line,
    CASE WHEN lower(A.text) LIKE '%dbms_%' THEN 1 ELSE 0 END dbms,
    CASE WHEN lower(A.text) LIKE '%dbms_sql%' THEN 1 ELSE 0 END dbms_sql,
    CASE WHEN lower(A.text) LIKE '%execute%immediate%' THEN 1 ELSE 0 END exec_im,
    CASE WHEN lower(A.text) LIKE '%utl_%' THEN 1 ELSE 0 END utl
  FROM cdb_source A
) B
GROUP BY
  B.con_id,
  B.owner,
  B.name,
  B.type
