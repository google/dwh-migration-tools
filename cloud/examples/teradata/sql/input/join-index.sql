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

CREATE TABLE customer(
  c_custkey INTEGER,
  c_name CHARACTER(26) NOT NULL,
  c_address VARCHAR(41),
  c_nationkey INTEGER,
  c_phone CHARACTER(16),
  c_acctbal DECIMAL(13, 2),
  c_mktsegment CHARACTER(21),
  c_comment VARCHAR(127))
  PRIMARY INDEX(c_custkey);

CREATE TABLE orders(
  o_orderkey INTEGER,
  o_date DATE FORMAT 'yyyy-mm-dd',
  o_status CHARACTER(1),
  o_custkey INTEGER,
  o_totalprice DECIMAL(13, 2),
  o_orderpriority CHARACTER(21),
  o_clerk CHARACTER(16),
  o_shippriority INTEGER,
  o_comment VARCHAR(79))
  UNIQUE PRIMARY INDEX(o_orderkey);

CREATE
JOIN INDEX ord_cust_idx, FALLBACK, CHECKSUM = DEFAULT, MAP = SmallTableMap AS
SELECT (o_custkey, c_name), (o_status, o_date, o_comment)
FROM orders, customer
WHERE o_custkey = c_custkey;
