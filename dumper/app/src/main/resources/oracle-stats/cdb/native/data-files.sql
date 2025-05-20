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
  DataFiles.con_id "ConId",
  DataFiles.file_id "FileId",
  DataFiles.bytes "Bytes",
  DataFiles.blocks "Blocks",
  DataFiles.status "Status",
  DataFiles.tablespace_name "TablespaceName",
  DataFiles.maxbytes "MaxBytes",
  DataFiles.maxblocks "MaxBlocks"
FROM cdb_data_files DataFiles
