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

--/** XXX #11
CREATE
  MACRO GetZipCodeRiskFactors(ZipCode INTEGER)
AS (
  SELECT FireRisk, FloodRisk, TheftRisk, SubsidenceRisk, OtherRisk
  FROM ZipCodeRiskFactors WHERE:ZipCode BETWEEN StartZipRange AND EndZipRange;

);

COMMIT;

--/**
-- *
-- * Stored Procedure to Get the Risk Factors associated with a given ZipCode
-- *
-- */
CREATE
  PROCEDURE
    "GetZipCodeRiskFactorsSP"
      (
        IN ZipCode INTEGER,
        OUT FireRisk FLOAT,
        OUT FloodRisk FLOAT,
        OUT TheftRisk FLOAT,
        OUT SubsidenceRisk FLOAT,
        OUT OtherRisk FLOAT)
        BEGIN
SELECT
  FireRisk,
  FloodRisk,
  TheftRisk,
  SubsidenceRisk,
  OtherRisk
    INTO FireRisk,
  FloodRisk,
  TheftRisk,
  SubsidenceRisk,
  OtherRisk
FROM ZipCodeRiskFactors
WHERE ZipCode BETWEEN StartZipRange AND EndZipRange;

END;

COMMIT;

--CREATE PROCEDURE "GetCustomerID_Slow" (OUT CUSTOMER_ID INTEGER)
--BEGIN
--    DECLARE workingID INTEGER;
--    SELECT MAX(CUSTOMER_ID) INTO workingID FROM CUSTOMER;
--    IF workingID IS NULL THEN
--        SET workingID = 1;
--    ELSE
--        SET workingID = workingID + 1;
--    END IF;
--    SET CUSTOMER_ID = workingID;
--END;

CREATE
  PROCEDURE "GetCustomerID_Medium" (OUT CUSTOMER_ID INTEGER) BEGIN DECLARE workingID INTEGER;

SELECT CUSTOMER_ID INTO workingID FROM CUSTOMER_INDEX WHERE LOCK_POINT = 1;

IF workingID
IS NULL THEN
SET workingID = 1;

INSERT CUSTOMER_INDEX (1, 1);

ELSE
SET workingID = workingID + 1;

INSERT CUSTOMER_INDEX (1, workingID);

END IF;

SET CUSTOMER_ID = workingID;

END;

--/**
-- *
-- * Stored Procedure to Get a Customer ID from the CUSTOMER_INDEX Table
-- *
-- */
CREATE
  PROCEDURE "GetCustomerID" (OUT CUSTOMER_ID INTEGER) BEGIN BEGIN REQUEST UPDATE CUSTOMER_INDEX
SET CUSTOMER_ID = CUSTOMER_ID + 1
WHERE
  LOCK_POINT = 1
    ELSE INSERT CUSTOMER_INDEX(1, 1);

SELECT CUSTOMER_ID INTO:CUSTOMER_ID FROM CUSTOMER_INDEX WHERE LOCK_POINT = 1;

END REQUEST;
END;

COMMIT;

--/**
-- *
-- * Stored Procedure to Get a Property ID from the PROPERTY_INDEX Table
-- *
-- */
CREATE
  PROCEDURE "GetPropertyID" (OUT PROPERTY_ID INTEGER) BEGIN BEGIN REQUEST UPDATE PROPERTY_INDEX
SET PROPERTY_ID = PROPERTY_ID + 1
WHERE
  LOCK_POINT = 1
    ELSE INSERT PROPERTY_INDEX(1, 1);

SELECT PROPERTY_ID INTO:PROPERTY_ID FROM PROPERTY_INDEX WHERE LOCK_POINT = 1;

END REQUEST;
END;

COMMIT;

--/**
-- *
-- * Stored Procedure to Get a Quotation ID from the QUOTATION_INDEX Table
-- *
-- */
CREATE
  PROCEDURE "GetQuotationID" (OUT QUOTATION_ID INTEGER) BEGIN BEGIN REQUEST UPDATE QUOTATION_INDEX
SET QUOTATION_ID = QUOTATION_ID + 1
WHERE
  LOCK_POINT = 1
    ELSE INSERT QUOTATION_INDEX(1, 1);

SELECT QUOTATION_ID INTO:QUOTATION_ID FROM QUOTATION_INDEX WHERE LOCK_POINT = 1;

END REQUEST;
END;

COMMIT;
