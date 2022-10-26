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

DROP TABLE IF EXISTS demo_with_cte;

CREATE TABLE demo_with_cte(a_integer int, a_date date);

-- Transpiler context keeps track of demo_with_cte columns through a CTE
WITH demo_cte AS (SELECT * FROM demo_with_cte)
SELECT a_integer + 1, a_date + 1 FROM demo_cte;
