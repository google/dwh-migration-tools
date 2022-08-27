-- Copyright 2022 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

DROP TABLE IF EXISTS demo_select_top_n;

CREATE TABLE demo_select_top_n(a_integer int, a_date date);

-- Not all dialects support "SELECT TOP N" form, some use the "SELECT .. FROM .. LIMIT N" form
SELECT top 2 * FROM demo_select_top_n;
