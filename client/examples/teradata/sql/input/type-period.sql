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

DROP TABLE IF EXISTS demo_type_period;

CREATE TABLE demo_type_period(a_date_period period(date), another_date_period period(date));

-- Period type is not supported on some target dialects, it's emulated using other data types (e.g.: struct, array, ...)
INSERT INTO demo_type_period
VALUES (period '(2010-01-01, 2012-02-02)', period '(2011-01-01, 2014-02-02)');

-- The overlaps operator will be transpiled as conjunction of period boundary checks
SELECT * FROM demo_type_period WHERE a_date_period overlaps another_date_period;
