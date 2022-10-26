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

DROP TABLE IF EXISTS ${demo_date_truncation_table};

CREATE TABLE ${demo_date_truncation_table}(a_date date);

INSERT INTO ${demo_date_truncation_table} VALUES (date '2010-05-16');

-- Some dialects don't support truncation to century or rounding of a date to the quarter, this will be emulated.
SELECT trunc(a_date, 'CC'), round(a_date, 'Q')
FROM ${demo_date_truncation_table}
WHERE a_date = ${demo_date};
