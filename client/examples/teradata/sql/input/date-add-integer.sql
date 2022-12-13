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

DROP TABLE IF EXISTS demo_date_add_integer;

CREATE TABLE demo_date_add_integer(a_integer int, a_date date);

-- In Teradata "date + 1" adds one day to the date, the transpiler may emit a function (e.g.: date_add) in target dialect
SELECT a_integer + 1, a_date + 1 FROM demo_date_add_integer;
