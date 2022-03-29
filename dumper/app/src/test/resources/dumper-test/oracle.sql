-- Copyright 2022 Google LLC
-- Copyright 2013-2021 CompilerWorks
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
CREATE USER test2 IDENTIFIED BY test;
GRANT connect, resource to test2;
GRANT CREATE VIEW to test2;
GRANT CREATE MATERIALIZED VIEW to test2;
GRANT unlimited tablespace TO test2;
CREATE USER test3 IDENTIFIED BY test;
GRANT connect, resource to test3;
GRANT unlimited tablespace TO test3;
GRANT CREATE VIEW to test3;
GRANT CREATE MATERIALIZED VIEW to test3;
CREATE USER test_dba2 IDENTIFIED BY test;
GRANT DBA to test_dba2;

CREATE TABLE table1 (d1 CHAR, d2 VARCHAR2(10), d3 NCHAR, d4 NVARCHAR2(10), d5 NUMBER, d6 FLOAT, d7 BINARY_FLOAT, d8 BINARY_DOUBLE, d9 LONG, d10 RAW(10), d11 DATE, d12 TIMESTAMP, d13 TIMESTAMP WITH TIME ZONE, d14 TIMESTAMP WITH LOCAL TIME ZONE, d15 INTERVAL YEAR(2) TO MONTH, d16 INTERVAL DAY(2) TO SECOND(2), d17 ROWID, d18 UROWID(10), d19 CLOB, d20 NCLOB, d21 BLOB, d22 BFILE );

CREATE TABLE table2 (d1 CHAR NOT NULL, d2 VARCHAR2(10) NOT NULL, d3 NCHAR NOT NULL, d4 NVARCHAR2(10) NOT NULL, d5 NUMBER NOT NULL, d6 FLOAT NOT NULL, d7 BINARY_FLOAT NOT NULL, d8 BINARY_DOUBLE NOT NULL, d9 LONG NOT NULL, d10 RAW(10) NOT NULL, d11 DATE NOT NULL, d12 TIMESTAMP NOT NULL, d13 TIMESTAMP WITH TIME ZONE NOT NULL, d14 TIMESTAMP WITH LOCAL TIME ZONE NOT NULL, d15 INTERVAL YEAR(2) TO MONTH NOT NULL, d16 INTERVAL DAY(2) TO SECOND(2) NOT NULL, d17 ROWID NOT NULL, d18 UROWID(10) NOT NULL, d19 CLOB NOT NULL, d20 NCLOB NOT NULL, d21 BLOB NOT NULL, d22 BFILE NOT NULL );

CREATE TABLE table3 (d1 CHAR(10) DEFAULT 'foo', d2 VARCHAR2(10) DEFAULT 'foo', d3 NCHAR(10) DEFAULT 'foo', d4 NVARCHAR2(10) DEFAULT 'foo', d5 NUMBER DEFAULT 1, d6 FLOAT DEFAULT 1.0);

CREATE TABLE table4 (key_column VARCHAR2(10) PRIMARY KEY, xml_column XMLType);

CREATE OR REPLACE TYPE test_type IS VARRAY(10) of VARCHAR2(15);

CREATE TABLE table5 (key_column VARCHAR2(10) PRIMARY KEY, test test_type);

CREATE TABLE table6
  (id          VARCHAR2 (32) NOT NULL PRIMARY KEY,
   date_loaded TIMESTAMP (6) WITH TIME ZONE,
   json_document VARCHAR2 (2376)
   CONSTRAINT ensure_json CHECK (json_document IS JSON));

CREATE VIEW test_view AS
  SELECT d1, d2, d3, d4
  FROM table1;

CREATE MATERIALIZED VIEW test_view2 BUILD IMMEDIATE AS SELECT d1, d2, d3, d4, d5 FROM table2;

create or replace FUNCTION get_complete_address(in_person_id IN NUMBER)
   RETURN VARCHAR2
   IS person_details VARCHAR2(130);

CREATE TYPE lineitem AS OBJECT (
  item_name   VARCHAR2(30),
  quantity    NUMBER,
  unit_price  NUMBER(12,2) );

CREATE TYPE lineitem_table AS TABLE OF lineitem;

CREATE TYPE SDO_GEOMETRY AS OBJECT (
   sgo_gtype        NUMBER,
   sdo_srid         NUMBER,
   sdo_point        SDO_POINT_TYPE,
   sdo_elem_info    SDO_ELEM_INFO_ARRAY,
   sdo_ordinates    SDO_ORDINATE_ARRAY);

CREATE TYPE SDO_TOPO_GEOMETRY AS OBJECT (
   tg_type          NUMBER,
   tg_id            NUMBER,
   tg_layer_id      NUMBER,
   topology_id      NUMBER);

CREATE TYPE SDO_GEORASTER AS OBJECT (
   rasterType      NUMBER,
   spatialExtent   SDO_GEOMETRY,
   rasterDataTable VARCHAR2(32),
   rasterID        NUMBER,
   metadata        XMLType);

CREATE TABLE table5 (key_column VARCHAR2(10) PRIMARY KEY, test SDO_GEOMETRY, test2 SDO_TOPO_GEOMETRY, test3 SDO_GEORASTER);



-- Make some metadata


-- 03Apr: 01 -- Keeping empty to know list of system schema
--              so that they can be filter out.

