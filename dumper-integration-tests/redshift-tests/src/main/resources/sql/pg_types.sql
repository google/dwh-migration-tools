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
  pg_catalog.format_type(t.oid, NULL) AS "name",
  pg_catalog.obj_description(t.oid, 'pg_type') as "description"
  FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_namespace n
  ON n.oid = t.typnamespace
  WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem ) AND pg_catalog.pg_type_is_visible(t.oid) ORDER BY 1, 2;