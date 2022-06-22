-- Copyright 2022 Google LLC
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

SELECT n.nspname as "schema",
p.proname AS "name",
pg_catalog.format_type(p.prorettype, NULL) AS "result data type",
CASE WHEN p.pronargs = 0 THEN CAST('*' AS pg_catalog.text)
ELSE oidvectortypes(p.proargtypes) END AS "argument data types",
pg_catalog.obj_description(p.oid, 'pg_proc') as "description"
FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n
ON n.oid = p.pronamespace
WHERE p.proisagg AND pg_catalog.pg_function_is_visible(p.oid) ORDER BY 1, 2, 4;