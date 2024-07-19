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
  A.dbid "DbId",
  A.name "Name",
  A.version "Version",
  A.detected_usages "DetectedUsages",
  A.total_samples "TotalSamples",
  A.currently_used "CurrentlyUsed",
  A.first_usage_date "FirstUsageDate",
  A.last_usage_date "LastUsageDate",
  A.aux_count "AuxCount",
  A.feature_info "FeatureInfo",
  A.last_sample_date "LastSampleDate",
  A.last_sample_period "LastSamplePeriod",
  A.sample_interval "SampleInterval",
  A.description "Description",
  A.con_id "ConId"
FROM cdb_feature_usage_statistics A
ORDER BY A.name
