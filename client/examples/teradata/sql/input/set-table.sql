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

DROP TABLE IF EXISTS foo.bar;

CREATE SET TABLE foo.bar(x int, y int);

DROP TABLE IF EXISTS foo.duplicate_rows;

CREATE TABLE foo.duplicate_rows(r0 int, r1 int);

INSERT INTO foo.duplicate_rows VALUES (41, 102);
INSERT INTO foo.duplicate_rows VALUES (41, 102);
INSERT INTO foo.duplicate_rows VALUES (42, 102);
INSERT INTO foo.bar (x, y) SELECT r0, r1 FROM foo.duplicate_rows dup;

SELECT * FROM foo.bar f;
