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
  SysMetrics.con_id "ConId",
  SysMetrics.con_dbid "ConDbId",
  SysMetrics.dbid "DbId",
  SysMetrics.begin_time "BeginTime",
  SysMetrics.end_time "EndTime",
  SysMetrics.metric_id "MetricId",
  SysMetrics.metric_name "MetricName",
  SysMetrics.value "Value",
  SysMetrics.metric_unit "MetricUnit"
FROM cdb_hist_sysmetric_history SysMetrics
  -- use a query parameter to get the number of querylog days that should be loaded
  WHERE SysMetrics.end_time > sysdate - ?
