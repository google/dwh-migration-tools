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

DROP TABLE CUSTOMER_INDEX;

COMMIT;

DROP TABLE PROPERTY_INDEX;

COMMIT;

DROP TABLE QUOTATION_INDEX;

COMMIT;

DROP PROCEDURE GetCustomerID_Slow;

COMMIT;

DROP PROCEDURE GetCustomerID_Medium;

COMMIT;

DROP PROCEDURE GetCustomerID;

COMMIT;

DROP PROCEDURE GetPropertyID;

COMMIT;

DROP PROCEDURE GetQuotationID;

COMMIT;

DROP TABLE QUOTATION;

COMMIT;

DROP TABLE PROPERTY;

COMMIT;

DROP TABLE CUSTOMER;

COMMIT;

DROP MACRO GetZipCodeRiskFactors;
DROP PROCEDURE GetZipCodeRiskFactorsSP;

COMMIT;

DROP TABLE ZipCodeRiskFactors;

COMMIT;
