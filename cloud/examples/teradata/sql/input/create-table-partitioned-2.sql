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

DROP TABLE IF EXISTS store_revenue;

CREATE TABLE store_revenue(
  store_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  sales_date DATE FORMAT 'yyyy-mm-dd' NOT NULL,
  total_revenue DECIMAL(13, 2),
  total_sold INTEGER,
  note VARCHAR(256))
PARTITION BY(
  CASE_N(
    store_id < 100,
    store_id < 500,
    store_id < 999,
    NO CASE),
  CASE_N(
    sales_date < DATE '2001-11-11',
    sales_date < DATE '2012-11-11',
    NO CASE));
