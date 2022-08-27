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

--create database "TZA_DB" from "DBC"
--    permanent = 104857600,
--    spool = 104857600,
--    temporary = 104857600,
--    no fallback,
--    no before journal,
--    no after journal;

-- https://downloads.teradata.com/enterprise/articles/the-friday-night-project
-- XXX #3

-- XXX #?
-- You Will need DBC access to do some of these.
GRANT ALL ON TZA_DB TO TZA_USER WITH GRANT OPTION;

COMMIT;

GRANT EXECUTE ON TZA_DB TO TZA_USER;

COMMIT;

GRANT
  CREATE
    EXTERNAL PROCEDURE
ON TZA_DB
TO TZA_USER;

COMMIT;

GRANT EXECUTE PROCEDURE ON SQLJ TO TZA_USER;

COMMIT;

CREATE TABLE ZipCodeRiskFactors(
  ID integer NOT NULL
  PRIMARY KEY,
  StartZipRange integer NOT NULL,
  EndZipRange integer NOT NULL,
  FireRisk float
  DEFAULT
    1.0,
    FloodRisk float
  DEFAULT
    1.0,
    TheftRisk float
  DEFAULT
    1.0,
    SubsidenceRisk float
  DEFAULT
    1.0,
    OtherRisk float
  DEFAULT 1.0)
  UNIQUE INDEX(StartZipRange, EndZipRange);

-- XXX #6
INSERT INTO ZipCodeRiskFactors VALUES (1, '35000', '36999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- AL
INSERT INTO ZipCodeRiskFactors VALUES (2, '99500', '99999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- AK
INSERT INTO ZipCodeRiskFactors VALUES (3, '71600', '72999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- AR
INSERT INTO ZipCodeRiskFactors VALUES (4, '85000', '86599', 1.0, 1.0, 1.0, 1.0, 1.0);  -- AZ
INSERT INTO ZipCodeRiskFactors VALUES (5, '90000', '96299', 1.0, 1.0, 1.0, 1.0, 1.0);  -- CA
INSERT INTO ZipCodeRiskFactors VALUES (6, '80000', '81699', 1.0, 1.0, 1.0, 1.0, 1.0);  -- CO
INSERT INTO ZipCodeRiskFactors VALUES (7, '06000', '06999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- CT
INSERT INTO ZipCodeRiskFactors VALUES (8, '19700', '19999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- DE
INSERT INTO ZipCodeRiskFactors VALUES (9, '32000', '34999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- FL
INSERT INTO ZipCodeRiskFactors VALUES (10, '30000', '31999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- GA
INSERT INTO ZipCodeRiskFactors VALUES (11, '39800', '39999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- GA
INSERT INTO ZipCodeRiskFactors VALUES (12, '96700', '96899', 1.0, 1.0, 1.0, 1.0, 1.0);  -- HI
INSERT INTO ZipCodeRiskFactors VALUES (13, '50000', '52899', 1.0, 1.0, 1.0, 1.0, 1.0);  -- IA
INSERT INTO ZipCodeRiskFactors VALUES (14, '83200', '83899', 1.0, 1.0, 1.0, 1.0, 1.0);  -- ID
INSERT INTO ZipCodeRiskFactors VALUES (15, '60000', '62999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- IL
INSERT INTO ZipCodeRiskFactors VALUES (16, '46000', '47999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- IN
INSERT INTO ZipCodeRiskFactors VALUES (17, '66000', '67999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- KS
INSERT INTO ZipCodeRiskFactors VALUES (18, '40000', '42799', 1.0, 1.0, 1.0, 1.0, 1.0);  -- KY
INSERT INTO ZipCodeRiskFactors VALUES (19, '70000', '71499', 1.0, 1.0, 1.0, 1.0, 1.0);  -- LA
INSERT INTO ZipCodeRiskFactors VALUES (20, '05500', '05599', 1.0, 1.0, 1.0, 1.0, 1.0);  -- MA
INSERT INTO ZipCodeRiskFactors VALUES (21, '01000', '02799', 1.0, 1.0, 1.0, 1.0, 1.0);  -- MA
INSERT INTO ZipCodeRiskFactors VALUES (22, '20311', '20311', 1.0, 1.0, 1.0, 1.0, 1.0);  -- MD
INSERT INTO ZipCodeRiskFactors VALUES (23, '20335', '21999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- MD
INSERT INTO ZipCodeRiskFactors VALUES (24, '03900', '04999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- ME
INSERT INTO ZipCodeRiskFactors VALUES (25, '48000', '49999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- MI
INSERT INTO ZipCodeRiskFactors VALUES (26, '55000', '56799', 1.0, 1.0, 1.0, 1.0, 1.0);  -- MN
INSERT INTO ZipCodeRiskFactors VALUES (27, '63000', '65899', 1.0, 1.0, 1.0, 1.0, 1.0);  -- MO
INSERT INTO ZipCodeRiskFactors VALUES (28, '38600', '39799', 1.0, 1.0, 1.0, 1.0, 1.0);  -- MS
INSERT INTO ZipCodeRiskFactors VALUES (29, '71233', '71233', 1.0, 1.0, 1.0, 1.0, 1.0);  -- MS
INSERT INTO ZipCodeRiskFactors VALUES (30, '59000', '59999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- MT
INSERT INTO ZipCodeRiskFactors VALUES (31, '27000', '28999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- NC
INSERT INTO ZipCodeRiskFactors VALUES (32, '58000', '58899', 1.0, 1.0, 1.0, 1.0, 1.0);  -- ND
INSERT INTO ZipCodeRiskFactors VALUES (33, '68000', '69399', 1.0, 1.0, 1.0, 1.0, 1.0);  -- NE
INSERT INTO ZipCodeRiskFactors VALUES (34, '03000', '03899', 1.0, 1.0, 1.0, 1.0, 1.0);  -- NH
INSERT INTO ZipCodeRiskFactors VALUES (35, '07000', '08999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- NJ
INSERT INTO ZipCodeRiskFactors VALUES (36, '87000', '88499', 1.0, 1.0, 1.0, 1.0, 1.0);  -- NM
INSERT INTO ZipCodeRiskFactors VALUES (37, '88900', '89899', 1.0, 1.0, 1.0, 1.0, 1.0);  -- NV
INSERT INTO ZipCodeRiskFactors VALUES (38, '10000', '14999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- NY
INSERT INTO ZipCodeRiskFactors VALUES (39, '00400', '00599', 1.0, 1.0, 1.0, 1.0, 1.0);  -- NY
INSERT INTO ZipCodeRiskFactors VALUES (40, '06390', '06390', 1.0, 1.0, 1.0, 1.0, 1.0);  -- NY
INSERT INTO ZipCodeRiskFactors VALUES (41, '43000', '45999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- OH
INSERT INTO ZipCodeRiskFactors VALUES (42, '73400', '74999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- OK
INSERT INTO ZipCodeRiskFactors VALUES (43, '73000', '73199', 1.0, 1.0, 1.0, 1.0, 1.0);  -- OK
INSERT INTO ZipCodeRiskFactors VALUES (44, '97000', '97999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- OR
INSERT INTO ZipCodeRiskFactors VALUES (45, '15000', '19699', 1.0, 1.0, 1.0, 1.0, 1.0);  -- PA
INSERT INTO ZipCodeRiskFactors VALUES (46, '02800', '02999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- RI
INSERT INTO ZipCodeRiskFactors VALUES (47, '29000', '29999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- SC
INSERT INTO ZipCodeRiskFactors VALUES (48, '57000', '57799', 1.0, 1.0, 1.0, 1.0, 1.0);  -- SD
INSERT INTO ZipCodeRiskFactors VALUES (49, '37000', '38599', 1.0, 1.0, 1.0, 1.0, 1.0);  -- TN
INSERT INTO ZipCodeRiskFactors VALUES (50, '75000', '79999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- TX
INSERT INTO ZipCodeRiskFactors VALUES (51, '84000', '84799', 1.0, 1.0, 1.0, 1.0, 1.0);  -- UT
INSERT INTO ZipCodeRiskFactors VALUES (52, '20040', '20199', 1.0, 1.0, 1.0, 1.0, 1.0);  -- VA
INSERT INTO ZipCodeRiskFactors VALUES (53, '20301', '20301', 1.0, 1.0, 1.0, 1.0, 1.0);  -- VA
INSERT INTO ZipCodeRiskFactors VALUES (54, '20370', '20370', 1.0, 1.0, 1.0, 1.0, 1.0);  -- VA
INSERT INTO ZipCodeRiskFactors VALUES (55, '05000', '05499', 1.0, 1.0, 1.0, 1.0, 1.0);  -- VT
INSERT INTO ZipCodeRiskFactors VALUES (56, '05600', '05699', 1.0, 1.0, 1.0, 1.0, 1.0);  -- VT
INSERT INTO ZipCodeRiskFactors VALUES (57, '98000', '99499', 1.0, 1.0, 1.0, 1.0, 1.0);  -- WA
INSERT INTO ZipCodeRiskFactors VALUES (58, '53000', '54999', 1.0, 1.0, 1.0, 1.0, 1.0);  -- WI
INSERT INTO ZipCodeRiskFactors VALUES (59, '24700', '26899', 1.0, 1.0, 1.0, 1.0, 1.0);  -- WV
INSERT INTO ZipCodeRiskFactors VALUES (60, '82000', '83199', 1.0, 1.0, 1.0, 1.0, 1.0);  -- WY

--/** XXX #16

--/**
-- * CUSTOMER Table used to hold Customer Information
-- */

CREATE TABLE CUSTOMER(
  CUSTOMER_ID INTEGER NOT NULL
  PRIMARY KEY,
  FIRSTNAME VARCHAR(50),
  MIDDLENAME VARCHAR(50),
  LASTNAME VARCHAR(50),
  DOB DATE NOT NULL,
  PREFIX VARCHAR(4),
  SALUTATION VARCHAR(10))
  UNIQUE INDEX(FIRSTNAME, MIDDLENAME, LASTNAME);

COMMIT;

--/**
-- * PROPERTY Table used to hold Property Information
-- */

CREATE TABLE PROPERTY(
  PROPERTY_ID INTEGER NOT NULL
  PRIMARY KEY,
  HOUSE_NAMENUMBER VARCHAR(15),
  STREET_ADDRESS1 VARCHAR(50) NOT NULL,
  STREET_ADDRESS2 VARCHAR(50),
  STREET_ADDRESS3 VARCHAR(50),
  STREET_ADDRESS4 VARCHAR(50),
  CITY VARCHAR(50) NOT NULL,
  STATE CHAR(2) NOT NULL,
  ZIPCODE CHAR(5) NOT NULL,
  --/*
  -- * OWNERSHIP:- 0 - RENTED, 1 - OWNED, 2 - MORTGAGED.
  -- */
  OWNERSHIP INTEGER NOT NULL,
  NUM_BEDROOMS INTEGER NOT NULL,
  YEAR_BUILT CHAR(4) NOT NULL,
  --/*
  -- * PROPERTY_TYPE:- 0 - APARTMENT, 1 - CONDOMINIUM, 2 - TOWN HOUSE, 3 - DUPLEX, 4 - DETACHED.
  -- */
  PROPERTY_TYPE INTEGER NOT NULL,
  BUILDINGS_AMOUNT_INSURED INTEGER NOT NULL,
  BUILDINGS_COVER CHAR(1) NOT NULL,
  BUILDINGS_ACCIDENTAL_COVER CHAR(1) NOT NULL,
  CONTENTS_AMOUNT_INSURED INTEGER NOT NULL,
  CONTENTS_COVER CHAR(1) NOT NULL,
  CONTENTS_ACCIDENTAL_COVER CHAR(1) NOT NULL,
  SINGLE_ITEM_LIMIT INTEGER,
  ALARMED CHAR(1) NOT NULL,
  SECURITY_PATROLLED CHAR(1) NOT NULL,
  CUSTOMER_ID INTEGER NOT NULL,
  FOREIGN KEY(CUSTOMER_ID) REFERENCES CUSTOMER(CUSTOMER_ID))
  UNIQUE INDEX(HOUSE_NAMENUMBER, STREET_ADDRESS1, ZIPCODE);

COMMIT;

--/**
-- * QUOTATION Table holds information on Quotes given to a CUSTOMER against their PROPERTY
-- */

CREATE TABLE QUOTATION(
  QUOTATION_ID INTEGER NOT NULL
  PRIMARY KEY,
  PROPERTY_ID INTEGER NOT NULL,
  AMOUNT NUMERIC(18, 2) NOT NULL,
  CURRENCY_CODE CHAR(3) NOT NULL,
  START_DATE DATE NOT NULL,
  EXPIRY_DATE DATE NOT NULL,
  FOREIGN KEY(PROPERTY_ID) REFERENCES PROPERTY(PROPERTY_ID))
  UNIQUE INDEX(PROPERTY_ID);

COMMIT;

CREATE
  PROCEDURE "GetCustomerID_Slow" (OUT CUSTOMER_ID INTEGER) BEGIN DECLARE workingID INTEGER;

SELECT MAX(CUSTOMER_ID) INTO workingID FROM CUSTOMER;

IF workingID
IS NULL THEN
SET workingID = 1;

ELSE
SET workingID = workingID + 1;

END IF;

SET CUSTOMER_ID = workingID;

END;

--/**
-- * CUSTOMER_INDEX Table used to hold current Customer Index
-- */
CREATE TABLE CUSTOMER_INDEX(
  LOCK_POINT INTEGER NOT NULL
  PRIMARY KEY,
  CUSTOMER_ID INTEGER NOT NULL);

--/**
-- * PROPERTY_INDEX Table used to hold current Property Index
-- */
CREATE TABLE PROPERTY_INDEX(
  LOCK_POINT INTEGER NOT NULL
  PRIMARY KEY,
  PROPERTY_ID INTEGER NOT NULL);

--/**
-- * QUOTATION_INDEX Table used to hold current Quotation Index
-- */
CREATE TABLE QUOTATION_INDEX(
  LOCK_POINT INTEGER NOT NULL
  PRIMARY KEY,
  QUOTATION_ID INTEGER NOT NULL);
