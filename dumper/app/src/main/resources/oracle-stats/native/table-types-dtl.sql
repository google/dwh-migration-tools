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
  D.con_id "ConId",
  D.owner "Owner",
  D.table_name "TableName",
  D.partitioned "Partitioned",
  D.iot_type "IotType",
  D.nested "Nested",
  D.temporary "Temporary",
  D.secondary "Secondary",
  D.cluster_name "ClusterName",
  D.source "Source",
  E.partitioning_type "PartitioningType",
  E.subpartitioning_type "SubpartitioningType",
  E.partition_count "PartitionCount",
  F.count "SubpartitionCount"
FROM (
  SELECT
    A.con_id,
    A.owner,
    A.table_name,
    A.partitioned,
    A.iot_type,
    A.nested,
    A.temporary,
    A.secondary,
    A.cluster_name,
    'TAB' source
  FROM cdb_tables A
  WHERE A.owner <> 'SYSTEM' AND A.owner NOT LIKE '%SYS'
  UNION ALL SELECT
    B.con_id,
    B.owner,
    B.table_name,
    'NO' partitioned,
    NULL iot_type,
    'NO' nested,
    'N' temporary,
    'N' secondary,
    NULL cluster_name,
    'XML' source
  FROM cdb_xml_tables B
  WHERE B.owner <> 'SYSTEM' AND B.owner NOT LIKE '%SYS'
  UNION ALL SELECT
    C.con_id,
    C.owner,
    C.table_name,
    C.partitioned,
    C.iot_type,
    C.nested,
    C.temporary,
    C.secondary,
    C.cluster_name,
    'OBJ' source
  FROM cdb_object_tables C
  WHERE C.owner <> 'SYSTEM' AND C.owner NOT LIKE '%SYS'
) D
LEFT JOIN cdb_part_tables E
  ON D.con_id = E.con_id
  AND D.owner = E.owner
  AND D.table_name = E.table_name
LEFT JOIN (
  SELECT
    G.con_id,
    G.table_owner,
    G.table_name,
    count(1) count
  FROM cdb_tab_subpartitions G
  GROUP BY
    G.con_id,
    G.table_owner,
    G.table_name
) F
  ON D.con_id = F.con_id
  AND D.owner = F.table_owner
  AND D.table_name = F.table_name
