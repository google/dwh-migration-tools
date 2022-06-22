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

SELECT d.datname as "name",
pg_catalog.pg_get_userbyid(d.datdba) as "owner",
pg_catalog.pg_encoding_to_char(d.encoding) as "encoding",
pg_catalog.array_to_string(d.datacl, ',') AS "access privileges"
FROM pg_catalog.pg_database_info d ORDER BY 1;