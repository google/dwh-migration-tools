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

-- Teradata constant types are dependent on the magnitude of the constant:
-- This demonstates type handling across aggregates when there are many hidden, implicit casts and conversions.

-- This table is irrelevant, we just need a FROM clause.
DROP TABLE IF EXISTS t_sum;

CREATE TABLE t_sum(s0 int);

-- Tinyint
SELECT sum(CASE WHEN s0 IS NULL THEN 0 ELSE 1 END), min(1), max(1), avg(1) FROM t_sum;

-- Smallint
SELECT sum(CASE WHEN s0 IS NULL THEN 300 ELSE 350 END), min(351), max(352), avg(353) FROM t_sum;

-- Integer
SELECT
  sum(CASE WHEN s0 IS NULL THEN 1234567 ELSE 1234567 END), min(124568), max(124569), avg(124570)
FROM t_sum;
