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
DROP SCHEMA IF EXISTS DBC CASCADE;
CREATE SCHEMA DBC;

CREATE --MULTISET
TABLE DBC.DBQLogTbl -- ,FALLBACK ,
--     NO BEFORE JOURNAL,
--     NO AFTER JOURNAL,
--     CHECKSUM = DEFAULT
     (
      ProcID DECIMAL(5,0) NOT NULL -- FORMAT '-(5)9'
      ,CollectTimeStamp TIMESTAMP(2) NOT NULL -- FORMAT 'YYYY-MM-DDBHH:MI:SS'
      ,QueryID DECIMAL(18,0) NOT NULL --FORMAT '--Z(17)9'
--      ,UserID BYTE(4) NOT NULL,
--      ,AcctString VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
--      ,ExpandAcctString VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
--      ,SessionID INTEGER FORMAT '--,---,---,--9' NOT NULL,
--      ,LogicalHostID SMALLINT FORMAT 'ZZZ9' NOT NULL,
--      ,RequestNum INTEGER FORMAT '--,---,---,--9' NOT NULL,
--      ,InternalRequestNum INTEGER FORMAT '--,---,---,--9' NOT NULL,
--      ,LogonDateTime TIMESTAMP(2) FORMAT 'YYYY-MM-DDBHH:MI:SS' NOT NULL,
--      ,AcctStringTime FLOAT FORMAT '99:99:99',
--      ,AcctStringHour SMALLINT FORMAT '--9',
--      ,AcctStringDate DATE FORMAT 'YY/MM/DD',
--      ,LogonSource CHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC,
--      ,AppID CHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
--      ,ClientID CHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
--      ,ClientAddr CHAR(45) CHARACTER SET LATIN NOT CASESPECIFIC,
--      ,QueryBand VARCHAR(6160) CHARACTER SET UNICODE NOT CASESPECIFIC,
--      ,ProfileID BYTE(4),
--      ,StartTime TIMESTAMP(2) FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z' NOT NULL,
--      ,FirstStepTime TIMESTAMP(2) FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z' NOT NULL,
--      ,FirstRespTime TIMESTAMP(2) FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z',
--      ,LastStateChange TIMESTAMP(2) FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z',
--      ,NumSteps SMALLINT FORMAT '---,--9' NOT NULL,
--      ,NumStepswPar SMALLINT FORMAT '---,--9',
--      ,MaxStepsInPar SMALLINT FORMAT '---,--9',
--      ,NumResultRows FLOAT FORMAT '----,---,---,---,--9',
--      ,TotalIOCount FLOAT FORMAT '----,---,---,---,--9',
--      ,AMPCPUTime FLOAT FORMAT '----,---,---,---,--9.999',
--      ,ParserCPUTime FLOAT FORMAT '----,---,---,---,--9.999',
--      ,UtilityByteCount FLOAT FORMAT '----,---,---,---,--9',
--      ,UtilityRowCount FLOAT FORMAT '----,---,---,---,--9',
      ,ErrorCode INTEGER -- FORMAT '--,---,---,--9',
--      ,ErrorText VARCHAR(1024) CHARACTER SET UNICODE NOT CASESPECIFIC FORMAT 'X(255)',
--      ,WarningOnly CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
--      ,DelayTime INTEGER FORMAT '--,---,---,--9',
--      ,WDDelayTime INTEGER FORMAT '--,---,---,--9',
--      ,AbortFlag CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
--      ,CacheFlag CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
--      ,StatementType CHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      ,QueryText VARCHAR(10000) --CHARACTER SET UNICODE NOT CASESPECIFIC,
--      ,NumOfActiveAMPs INTEGER FORMAT '--,---,---,--9',
--      ,MaxAMPCPUTime FLOAT FORMAT '----,---,---,---,--9.999',
--      ,MaxCPUAmpNumber SMALLINT FORMAT '---,--9',
--      ,MinAmpCPUTime FLOAT FORMAT '----,---,---,---,--9.999',
--      ,MaxAmpIO FLOAT FORMAT '----,---,---,---,--9',
--      ,MaxIOAmpNumber SMALLINT FORMAT '---,--9',
--      ,MinAmpIO FLOAT FORMAT '----,---,---,---,--9',
--      ,SpoolUsage FLOAT FORMAT '----,---,---,---,--9',
--      ,WDID INTEGER FORMAT '--,---,---,--9',
--      ,OpEnvID INTEGER FORMAT '--,---,---,--9',
--      ,SysConID INTEGER FORMAT '--,---,---,--9',
--      ,LSN INTEGER FORMAT '--,---,---,--9',
--      ,NoClassification CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
--      ,WDOverride CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
--      ,SLGMet CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
--      ,ExceptionValue INTEGER FORMAT '--,---,---,--9',
--      ,FinalWDID INTEGER FORMAT '--,---,---,--9',
--      ,TDWMEstMaxRows FLOAT FORMAT '----,---,---,---,--9',
--      ,TDWMEstLastRows FLOAT FORMAT '----,---,---,---,--9',
--      ,TDWMEstTotalTime FLOAT FORMAT '----,---,---,---,--9',
--      ,TDWMAllAmpFlag CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
--      ,TDWMConfLevelUsed CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
--      ,TDWMRuleID INTEGER FORMAT '--,---,---,--9',
      ,UserName VARCHAR(128) --CHARACTER SET UNICODE NOT CASESPECIFIC,
--      ,DefaultDatabase VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
--      ,AMPCPUTimeNorm FLOAT FORMAT '----,---,---,---,--9.999',
--      ,ParserCPUTimeNorm FLOAT FORMAT '----,---,---,---,--9.999',
--      ,MaxAMPCPUTimeNorm FLOAT FORMAT '----,---,---,---,--9.999',
--      ,MaxCPUAmpNumberNorm SMALLINT FORMAT '---,--9',
--      ,MinAmpCPUTimeNorm FLOAT FORMAT '----,---,---,---,--9.999',
--      ,EstResultRows FLOAT FORMAT '----,---,---,---,--9',
--      ,EstProcTime FLOAT FORMAT '----,---,---,---,--9.999',
--      ,EstMaxRowCount FLOAT FORMAT '----,---,---,---,--9',
--      ,ProxyUser VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
--      ,ProxyRole VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
--      ,StatementGroup VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
--      ,SessionTemporalQualifier VARCHAR(1024) CHARACTER SET LATIN NOT CASESPECIFIC,
--      ,ExtraField1 INTEGER FORMAT '--,---,---,--9',
--      ,ExtraField2 INTEGER FORMAT '--,---,---,--9',
--      ,ExtraField3 SMALLINT FORMAT '---,--9',
--      ,ExtraField4 TIMESTAMP(2) FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z',
--      ,ExtraField5 VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
--      ,ExtraField6 FLOAT FORMAT '----,---,---,---,--9',
--      ,ExtraField7 FLOAT FORMAT '----,---,---,---,--9'
);

-- PRIMARY INDEX ( ProcID ,CollectTimeStamp );
-- ALTER TABLE ONLY DBC.DBQLogTbl DROP CONSTRAINT IF EXISTS "DBQLOGTBL_PKEY";
-- ALTER TABLE ONLY DBC.DBQLogTbl ADD CONSTRAINT "DBQLOGTBL_PKEY" PRIMARY KEY ( ProcID, CollectTimeStamp );

-- NOTE: In TD this is a NUPI -- Non-Unique Primary Index; see:
-- https://docs.teradata.com/reader/B7Lgdw6r3719WUyiCSJcgw/lbBwAVG63valC2jh_k5cAQ
-- therefore in PostgreSQL we forgo the PK constraint and create only the index we're interested in:
CREATE INDEX DBQLogTbl_Index ON DBC.DBQLogTbl ( ProcID, CollectTimeStamp );

CREATE --MULTISET
TABLE DBC.DBQLSqlTbl -- ,FALLBACK ,
--     NO BEFORE JOURNAL,
--     NO AFTER JOURNAL,
--     CHECKSUM = DEFAULT
     (
      ProcID DECIMAL(5,0) NOT NULL --FORMAT '-(5)9'
      ,CollectTimeStamp TIMESTAMP(2) NOT NULL --FORMAT 'YYYY-MM-DDBHH:MI:SS'
      ,QueryID DECIMAL(18,0) NOT NULL --FORMAT '--Z(17)9'
      ,SqlRowNo INTEGER NOT NULL --FORMAT '--,---,---,--9'
      ,SqlTextInfo VARCHAR(31000) NOT NULL --CHARACTER SET UNICODE NOT CASESPECIFIC
);

-- PRIMARY INDEX ( ProcID ,CollectTimeStamp );
-- ALTER TABLE ONLY DBC.DBQLSqlTbl DROP CONSTRAINT IF EXISTS "DBQLSQLTBL_PKEY";
-- ALTER TABLE ONLY DBC.DBQLSqlTbl ADD CONSTRAINT "DBQLSQLTBL_PKEY" PRIMARY KEY ( ProcID, CollectTimeStamp );

-- NOTE: In TD this is a NUPI -- Non-Unique Primary Index; see:
-- https://docs.teradata.com/reader/B7Lgdw6r3719WUyiCSJcgw/lbBwAVG63valC2jh_k5cAQ
-- therefore in PostgreSQL we forgo the PK constraint and create only the index we're interested in:
CREATE INDEX DBQLSqlTbl_Index ON DBC.DBQLSqlTbl ( ProcID, CollectTimeStamp );
