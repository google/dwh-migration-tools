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

DROP TABLE IF EXISTS temp;

CREATE TABLE temp(col1 int, col2 date, col3 date);

DROP TABLE IF EXISTS temp2;

CREATE TABLE temp2(cti int, ctd date);

INSERT INTO temp (col1, col2, col3)
SELECT
  0 AS col1,
  (
    CASE
      WHEN cti > 0
        THEN
          CAST((TRIM(CAST((CAST((cti / 1000) AS INTEGER) + 1900) AS INTEGER)) || '-01-01') AS DATE)
          + (CAST(SUBSTR(CAST(cti AS VARCHAR(6)), 4, 3) AS INTEGER) - 1)
      ELSE (DATE '1600-01-01')
      END)
    col2,
  (
    ((EXTRACT(YEAR FROM ctd) + (CASE WHEN EXTRACT(MONTH FROM ctd) > 3 THEN 1 ELSE 0 END)) * 100)
    + EXTRACT(MONTH FROM ctd))
    col3
FROM
  temp2;
