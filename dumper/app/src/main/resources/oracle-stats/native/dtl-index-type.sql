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
  A.index_type "IndexType",
  A.uniqueness "Uniqueness",
  A.compression "Compression",
  A.partitioned "Partitioned",
  A.temporary "Temporary",
  A.secondary "Secondary",
  A.join_index "JoinIndex",
  CASE WHEN A.ityp_owner IS NOT NULL THEN 'Y' ELSE 'N' END "CustomIndexType",
  A.table_name "TableName",
  A.index_name "IndexName"
FROM cdb_indexes A
WHERE A.owner NOT LIKE '%SYS'
