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
  A.con_id "ConId",
  A.owner "Owner",
  A.partitioned "Partitioned",
  A.iot_type "IotType",
  A.nested "Nested",
  A.temporary "Temporary",
  A.secondary "Secondary",
  CASE
    WHEN A.cluster_name IS NULL THEN
      'N'
    ELSE
      'Y'
  END "ClusteredTable",
  count(1) "TableCount",
  'N' "ObjectTable",
  'N' "XmlTable"
FROM (
  SELECT
    '{prefix}'_source.con_id,
    cdb_source.owner,
    cdb_source.partitioned,
    cdb_source.iot_type,
    cdb_source.nested,
    cdb_source.temporary,
    cdb_source.secondary,
    cdb_source.cluster_name
  FROM (SELECT NULL con_id FROM dual) dba_source
  RIGHT OUTER JOIN '{prefix}'_tables cdb_source
  ON 0 = 0
) A
WHERE owner NOT LIKE '%SYS'
GROUP BY
  A.con_id,
  A.owner,
  A.partitioned,
  A.iot_type,
  A.nested,
  A.temporary,
  A.secondary,
  CASE
    WHEN cluster_name IS NULL THEN
      'N'
    ELSE
      'Y'
  END,
  'N',
  'N'
UNION ALL
SELECT
  B.con_id "ConId",
  B.owner "Owner",
  'NO' "Partitioned",
  NULL "IotType",
  'NO' "Nested",
  'N' "Temporary",
  'N' "Secondary",
  'N' "ClusteredTable",
  count(1) "TableCount",
  'N' "ObjectTable",
  'Y' "XmlTable"
FROM
(SELECT '{prefix}'_source.con_id, cdb_source.owner
  FROM '{prefix}'_xml_tables cdb_source
  LEFT JOIN (SELECT NULL con_id FROM dual) dba_source
  ON 0 = 0
  JOIN
  (SELECT count(1) cnt FROM '{prefix}'_views
  WHERE view_name = upper(''{prefix}'') || '_XML_TABLES')
  ON cnt > 0 AND cdb_source.owner NOT LIKE '%SYS'
) B
GROUP BY
  B.con_id,
  B.owner
