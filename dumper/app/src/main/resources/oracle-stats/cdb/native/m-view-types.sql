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
  A.updatable "Updatable",
  A.rewrite_enabled "RewriteEnabled",
  A.refresh_mode "RefreshMode",
  A.refresh_method "RefreshMethod",
  A.fast_refreshable "FastRefreshable",
  A.compile_state "CompileState",
  A.container_name "ContainerName",
  A.mview_name "MViewName"
FROM
  cdb_mviews A
WHERE A.owner NOT LIKE '%SYS'
