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

DROP TABLE IF EXISTS demo_qualify;

CREATE TABLE demo_qualify(a_integer int, a_date date);

-- QUALIFY clause is specific to Teradata, it will be transpiled with a subselect filtered on the qualify condition
SELECT a_date, rank() OVER (ORDER BY a_integer) AS q FROM demo_qualify QUALIFY q > 1;
