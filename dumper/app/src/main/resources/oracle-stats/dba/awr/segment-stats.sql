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
  NULL "ConId",
  SegStat.dbid "DbId",
  SegStat.instance_number "InstanceNumber",
  SegStat.ts# "TablespaceNumber",
  SegStat.obj# "DictionaryObjectNumber",
  SegStat.dataobj# "DataObjectNumber",
  Snap.begin_interval_time "BeginIntervalTime",
  Snap.end_interval_time "EndIntervalTime",
  SegStat.space_used_total "SpaceUsedTotal",
  SegStat.space_used_delta "SpaceUsedDelta",
  SegStat.space_allocated_total "SpaceAllocatedTotal",
  SegStat.space_allocated_delta "SpaceAllocatedDelta",
  SegStat.physical_reads_delta "PhysicalReadsBlocks",
  SegStat.physical_writes_delta "PhysicalWritesBlocks",
  SegStat.physical_read_requests_delta "PhysicalReadRequestsBlocks",
  SegStat.physical_write_requests_delta "PhysicalWriteRequestsBlocks",
  SegStat.table_scans_delta "TableScans",
  SegStatObj.owner "ObjectOwner",
  SegStatObj.object_name "ObjectName",
  SegStatObj.object_type "ObjectType"
FROM dba_hist_seg_stat SegStat
INNER JOIN dba_hist_seg_stat_obj SegStatObj
  ON SegStat.ts# = SegStatObj.ts#
  AND SegStat.obj# = SegStatObj.obj#
  AND SegStat.dataobj# = SegStatObj.dataobj#
INNER JOIN dba_hist_snapshot Snap
  ON SegStat.snap_id = Snap.snap_id
  AND SegStat.dbid = Snap.dbid
  AND SegStat.instance_number = Snap.instance_number
  -- use a query parameter to get the number of querylog days that should be loaded
  AND Snap.end_interval_time > sysdate - ?
