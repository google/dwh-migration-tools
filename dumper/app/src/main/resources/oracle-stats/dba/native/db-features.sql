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
  Statistics.dbid "DbId",
  Statistics.name "Name",
  Statistics.version "Version",
  Statistics.detected_usages "DetectedUsages",
  Statistics.total_samples "TotalSamples",
  Statistics.currently_used "CurrentlyUsed",
  Statistics.first_usage_date "FirstUsageDate",
  Statistics.last_usage_date "LastUsageDate",
  Statistics.aux_count "AuxCount",
  Statistics.feature_info "FeatureInfo",
  Statistics.last_sample_date "LastSampleDate",
  Statistics.last_sample_period "LastSamplePeriod",
  Statistics.sample_interval "SampleInterval",
  Statistics.description "Description"
FROM dba_feature_usage_statistics Statistics
