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

SELECT pg_catalog.format_type(castsource, NULL) AS "source type",
pg_catalog.format_type(casttarget, NULL) AS "target type",
CASE WHEN castfunc = 0 THEN '(binary coercible)'
ELSE p.proname END as "function",
CASE WHEN c.castcontext = 'e' THEN 'no'
WHEN c.castcontext = 'a' THEN 'in assignment'
ELSE 'yes' END as "implicit?" FROM pg_catalog.pg_cast c
LEFT JOIN pg_catalog.pg_proc p ON c.castfunc = p.oid
LEFT JOIN pg_catalog.pg_type ts ON c.castsource = ts.oid
LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid = ts.typnamespace
LEFT JOIN pg_catalog.pg_type tt ON c.casttarget = tt.oid
LEFT JOIN pg_catalog.pg_namespace nt ON nt.oid = tt.typnamespace
WHERE ( (true  AND pg_catalog.pg_type_is_visible(ts.oid))
OR (true  AND pg_catalog.pg_type_is_visible(tt.oid)) ) ORDER BY 1, 2;