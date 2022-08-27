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

DROP TABLE IF EXISTS ttt;

CREATE TABLE ttt(t_nx char(10), t_c char(10), t_v varchar(10), t_i int);

SELECT 1
FROM ttt
WHERE
  -- char
  t_c LIKE '0foo%' ESCAPE 'o'
  AND t_c NOT LIKE '2ba_' ESCAPE 'a'
  -- varchar
  AND t_v LIKE '3foo%' ESCAPE 'o'
  AND t_v NOT LIKE '4ba_' ESCAPE 'a'
  -- bad type
  AND to_char(t_i) LIKE '5foo%' ESCAPE 'o'
  AND to_char(t_i) NOT LIKE '6ba_' ESCAPE 'a'
  -- error type, nonexistent column/symbol/etc
  AND t_nx LIKE '7foo%' ESCAPE 'o'
  AND t_nx NOT LIKE '8ba_' ESCAPE 'a';

DROP TABLE IF EXISTS test.tfoo;

CREATE TABLE test.tfoo(x char(30) casespecific, y char(30) NOT casespecific);

SELECT * FROM test.tfoo WHERE x = 'foo' AND y = 'bar';

WITH
  f(x, y) AS (SELECT 1, 'foo'),
  g(x, y) AS (SELECT 2, 'FOO'),
  h(x, y) AS (SELECT x, y FROM f UNION ALL SELECT x, y FROM g)
SELECT min(x), max(x), y FROM h GROUP BY y;

WITH
  f(x, y) AS (SELECT 1, 'foo'),
  g(x, y) AS (SELECT 2, 'FOO'),
  h(x, y) AS (SELECT x, y FROM f UNION ALL SELECT x, y FROM g)
SELECT min(x), max(x), y FROM h GROUP BY 3;

WITH f(x) AS (SELECT 'Foo' (NOT casespecific))
SELECT x FROM f WHERE x IN ('foo', 'bar', 'baz');

WITH f(x) AS (SELECT 'FoO')
SELECT x FROM f WHERE x LIKE 'fO%';

WITH
  f(x, y) AS (SELECT 1, 'foo'),
  g(x, y) AS (SELECT 2, 'FOO')
SELECT x FROM f WHERE y IN (SELECT y FROM g);
