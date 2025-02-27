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
  NULL "ConId",
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
    A.owner,
    A.table_name,
    A.partitioned,
    A.iot_type,
    A.nested,
    A.temporary,
    A.secondary,
    A.cluster_name,
    'TAB' source
  FROM dba_tables A
  UNION ALL SELECT
    B.owner,
    B.table_name,
    'NO' partitioned,
    NULL iot_type,
    'NO' nested,
    'N' temporary,
    'N' secondary,
    NULL cluster_name,
    'XML' source
  FROM dba_xml_tables B
  UNION ALL SELECT
    C.owner,
    C.table_name,
    C.partitioned,
    C.iot_type,
    C.nested,
    C.temporary,
    C.secondary,
    C.cluster_name,
    'OBJ' source
  FROM dba_object_tables C
) D
LEFT JOIN dba_part_tables E
  ON D.owner = E.owner
  AND D.table_name = E.table_name
LEFT JOIN (
  SELECT
    G.table_owner,
    G.table_name,
    count(1) count
  FROM dba_tab_subpartitions G
  GROUP BY
    G.table_owner,
    G.table_name
) F
  ON D.owner = F.table_owner
  AND D.table_name = F.table_name
