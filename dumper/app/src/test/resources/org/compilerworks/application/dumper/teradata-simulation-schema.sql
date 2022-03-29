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

CREATE TABLE DBC.DBCInfoTbl (
    "InfoKey" VARCHAR(30),
    "InfoData" VARCHAR(16384)
);

CREATE VIEW DBC.DBCInfo AS
    SELECT
        "InfoKey",
        CAST(SUBSTRING("InfoData" FROM 1 FOR 16384) AS VARCHAR(16384)) AS "InfoData"
    FROM DBC.DBCInfoTbl;

CREATE VIEW DBC.DBCInfoV AS
    SELECT
        "InfoKey",
        DBCInfoTbl."InfoData" AS "InfoData"
    FROM DBC.DBCInfoTbl;

-- elasoft
CREATE TABLE DBC.DBase (
	"DatabaseNameI" varchar(256),
	"DatabaseId" bytea,	-- 4
	"OwnerId" bytea,	-- 4
	"EncryptionFlag" bytea,	-- 1
	"PasswordSalt" bytea,	-- 2
	"EncryptedPassword" bytea,	-- varbyte 512
	"PasswordModTime" Timestamp without Time Zone,
	"ProtectionType" char(1),
	"JournalFlag" char(2),
	"PermSpace" float8,
	"SpoolSpace" float8,
	"StartupString" varchar(510),
	"CommentString" varchar(510),
	"AccountName" varchar(256),
	"CreatorName" varchar(256),
	"DatabaseName" varchar(256),
	"JournalId" bytea,	-- 6
	"Version" smallint,
	"OwnerName" varchar(256),
	"NumFallBackTables" smallint,
	"NumLogProtTables" smallint,
	"DefaultDataBase" varchar(256),
	"LogonRules" smallint,
	"AccLogRules" smallint,
	"AccLogUsrRules" smallint,
	"DefaultCollation" char(1),
	"RowType" char(1),
	"PasswordChgDate" int,
	"LockedDate" int,
	"LockedTime" smallint,
	"LockedCount" char,	-- byteint
	"UnResolvedRICount" smallint,
	"TimeZoneHour" char,	-- byteint
	"TimeZoneMinute" char,	-- byteint
	"DefaultDateForm" char(1),
	"CreateUID" bytea,	-- 4
	"CreateTimeStamp" Timestamp without Time Zone,
	"LastAlterUID" bytea,	-- 4
	"LastAlterTimeStamp" Timestamp without Time Zone,
	"TempSpace" float8,
	"LastAccessTimeStamp" Timestamp without Time Zone,
	"AccessCount" int,
	"DefaultCharType" smallint,
	"RoleName" varchar(256),
	"ProfileName" varchar(256),
	"UDFLibRevision" int,
	"AppCat1Revision" int,
	"AppCat2Revision" int,
	"AppCat3Revision" int,
	"AppCat4Revision" int,
	"JarLibRevision" int,
	"TimeZoneString" varchar(256)
);

-- elasoft
CREATE TABLE DBC.DataBaseSpace (
	"DatabaseId" bytea,	-- 4
	"TableId" bytea,	-- 6
	"Vproc" smallint,
	"MaxPermSpace" float8,
	"MaxSpoolSpace" float8,
	"PeakPermSpace" float8,
	"PeakSpoolSpace" float8,
	"CurrentPermSpace" float8,
	"CurrentSpoolSpace" float8,
	"MaxTempSpace" float8,
	"PeakTempSpace" float8,
	"CurrentTempSpace" float8,
	"MaxProfileSpoolSpace" float8,
	"MaxProfileTempSpace" float8
);

-- elasoft
CREATE TABLE DBC.TVM (
	"DatabaseId" bytea,	-- 4
	"TVMNameI" varchar(256),
	"LogicalHostId" smallint,
	"SessionNo" int,
	"TVMId" bytea,	-- 6
	"TableKind" char(1),
	"ProtectionType" char(1),
	"TempFlag" char(1),
	"HashFlag" char(1),
	"NextIndexId" smallint,
	"NextFieldId" smallint,
	"Version" smallint,
	"RequestText" varchar(25000),
	"CreateText" varchar(26000),
	"CommentString" varchar(510),
	"CreatorName" varchar(256),
	"TVMName" varchar(256),
	"JournalFlag" char(2),
	"JournalId" bytea,	-- 6
	"UtilVersion" smallint,
	"AccLogRules" char(1),
	"ColumnAccRules" smallint,
	"CheckOpt" char(1),
	"ParentCount" smallint,
	"ChildCount" smallint,
	"NamedTblCheckCount" smallint,
	"UnnamedTblCheckExist" char(1),
	"PrimaryKeyIndexId" smallint,
	"CreateUID" bytea,	-- 4
	"CreateTimeStamp" Timestamp without Time Zone,
	"LastAlterUID" bytea,	-- 4
	"LastAlterTimeStamp" Timestamp without Time Zone,
	"TriggerCount" smallint,
	"CommitOpt" char(1),
	"TransLog" char(1),
	"LastAccessTimeStamp" Timestamp without Time Zone,
	"AccessCount" int,
	"SPObjectCodeRows" int,
	"RSGroupID" int,
	"TblRole" char(1),
	"TblStatus" char(1),
	"RequestTxtOverflow" char(1),
	"CreateTxtOverflow" char(1),
	"QueueFlag" char(1),
	"XSPExternalName" char(30),
	"XSPOptions" char(3),
	"XSPExtFileReference" varchar(2000),
	"ExecProtectionMode" char(1),
	"CharacterType" smallint,
	"Platform" char(8),
	"AuthIdUsed" bytea,	-- 6
	"AuthorizationType" char(1),
	"AuthorizationSubType" char(1),
	"OSDomainName" varchar(256),
	"OSUserName" varchar(256),
	"SecInfo" bytea,
	"AuthName" varchar(256),
	"TemporalProperty" char(1),
	"ResolvedCurrent_Date" date,
	"ResolvedCurrent_Timestamp" Timestamp with Time Zone,
	"SystemDefinedJI" char(1),
	"VTQualifier" char(1),
	"TTQualifier" char(1)
);

-- elasoft
create table DBC.tvfields (
	"TableId" bytea,	-- 6
	"FieldName" varchar(256),
	"FieldId" smallint,
	"Nullable" char(1),
	"FieldType" char(2),
	"MaxLength" int,
	"DefaultValue" varchar(2048),
	"DefaultValueI" bytea,	-- 1024
	"TotalDigits" smallint,
	"ImpliedPoint" smallint,
	"FieldFormat" varchar(256),
	"FieldTitle" varchar(512),
	"CommentString" varchar(510),
	"CollationFlag" char(1),
	"UpperCaseFlag" char(1),
	"DatabaseId" bytea,	-- 4
	"Compressible" char(1),
	"CompressValueList" varchar(16384),
	"FieldStatistics" bytea,	-- 16383
	"ColumnCheck" varchar(16384),
	"CheckCount" smallint,
	"CreateUID" bytea,	-- 4
	"CreateTimeStamp" Timestamp without Time Zone,
	"LastAlterUID" bytea,	-- 4
	"LastAlterTimeStamp" Timestamp without Time Zone,
	"LastAccessTimeStamp" Timestamp without Time Zone,
	"AccessCount" int,
	"SPParameterType" char(1),
	"CharType" smallint,
	"LobSequenceNo" smallint,
	"IdColType" char(2),
	"UDTypeId" bytea,	-- 6
	"UDTName" varchar(256),
	"TimeDimension" char(1),
	"VTCheckType" char(1),
	"TTCheckType" char(1),
	"ConstraintId" bytea	-- 4
);

-- elasoft
CREATE TABLE DBC.UDTInfo (
	"TypeId" bytea,	-- 6
	"DatabaseId" bytea,	-- 4
	"TypeName" varchar(256),
	"TypeKind" char(1),
	"INSTANTIABLE" char(1),
	"FINAL" char(1),
	"Encryption" char(1),
	"Compression" char(1),
	"OperatorAll" char(1),
	"DefaultTransformGroup" varchar(256),
	"OrderingForm" char(1),
	"OrderingCategory" char(1),
	"OrderingRoutineId" bytea,	-- 6
	"CastCount" char,	-- byteint
	"ExtFileReference" varchar(2000)
);

-- elasoft
CREATE TABLE DBC.Indexes (
	"TableId" bytea,	-- 6
	"IndexType" char(1),
	"IndexNumber" smallint,
	"UniqueFlag" char(1),
	"FieldId" smallint,
	"FieldPosition" smallint,
	"IndexMode" char(1),
	"DatabaseId" bytea,	-- 4
	"IndexStatistics" bytea,	-- varbyte 16383
	"Name" varchar(256),
	"CreateUID" bytea,	-- 4
	"CreateTimeStamp" Timestamp without Time Zone,
	"LastAlterUID" bytea,	-- 4
	"LastAlterTimeStamp" Timestamp without Time Zone,
	"LastAccessTimeStamp" Timestamp without Time Zone,
	"AccessCount" int,
	"JoinIndexTableID" bytea,	-- 6
	"UniqueOrPK" char(1),
	"VTConstraintType" char(1),
	"TTConstraintType" char(1),
	"SystemDefinedJI" char(1),
	"IndexTypeID" bytea,	-- 6
	"IndexVersion" smallint,
	"IndexParameterString" varchar(2048)
);

-- elasoft
CREATE TABLE DBC.UDFInfo (
	"DatabaseId" bytea,	-- 4
	"FunctionName" varchar(256),
	"FunctionId" bytea,	-- 6
	"NumParameters" smallint,
	"ParameterDataTypes" varchar(256),
	"FunctionType" char(1),
	"ExternalName" char(30),
	"SrcFileLanguage" char(1),
	"NoSQLDataAccess" char(1),
	"ParameterStyle" char(1),
	"DeterministicOpt" char(1),
	"NullCall" char(1),
	"PrepareCount" char(1),
	"ExecProtectionMode" char(1),
	"ExtFileReference" varchar(2000),
	"CharacterType" smallint,
	"Platform" char(8),
	"RoutineKind" char(1),
	"ParameterUDTIds" bytea,	-- varbyte 512
	"InterimFldSize" int,
	"MaxOutParameters" smallint,
	"AppCategory" char(1),
	"GLOPSetDatabaseName" varchar(256),
	"GLOPSetMemberName" varchar(256)
);

-- elasoft
CREATE TABLE DBC.TempTables (
    "HostNo" smallint,
    "SessionNo" int,
    "TableId" bytea,  -- 6
    "BaseDbId" bytea, -- 4
    "BaseTableId" bytea,  -- 6
    "AccountDbId" bytea,  -- 4
    "StatisticsCnt" smallint
);

-- elasoft
CREATE TABLE DBC.StatsTbl (
	"DatabaseId" bytea,	-- 4
	"TableId" bytea,	-- 6
	"ParentId" bytea,	-- 6
	"StatsId" int,
	"SampleSizePct" smallint,
	"IndexNumber" smallint,
	"StatsType" char(1),
	"DBSVersion" char(32),
	"ExpressionList" varchar(20000),
	"ExpressionListOverFlow" char(1),
	"ExpressionDataType" varchar(1000),
	"ExpressionCount" smallint,
	"FieldId" smallint,
	"PartitionList" varchar(5000),
	"RefObjects" varchar(10000),
	"RefObjectsOverFlow" char(1),
	"RefColumns" varchar(10000),
	"RefColumnsOverFlow" char(1),
	"GroupByList" varchar(10000),
	"GroupByListOverFlow" char(1),
	"BuiltInFcnValues" varchar(2000),
	"RefCount" int,
	"Source" char(1),
	"ObjectState" char(1),
	"RollingExpression" char(1),
	"NumAMPs" smallint,
	"CPUCost" float8,
	"EstPerDayGrowthPct" float8,
	"RowCount" float8,
	"BaseRowCount" float8,
	"UniqueValueCount" float8,
	"NullCount" float8,
	"AllNullCount" float8,
	"HighModeFreq" float8,
	"OneAMPRASDeviationPct" float8,
	"AllAMPRASDeviationPct" float8,
	"ThresholdPct" float8,
	"Entropy" float8,
	"ValidStats" char(1),
	"UsageType" char(1),
	"IntReserved1" int,
	"IntReserved2" int,
	"FloatReserved1" float8,
	"FloatReserved2" float8,
	"CharReserved1" char(5),
	"CharReserved2" char(5),
	"CharReserved3" char(1),
	"AccessCount" int,
	"LastAccessTimeStamp" Timestamp without Time Zone,
	"CreateUID" bytea,	-- 4
	"CreateTimeStamp" Timestamp without Time Zone,
	"LastAlterUID" bytea,	-- 4
	"LastAlterTimeStamp" Timestamp without Time Zone,
	"Histogram" bytea,	-- blob 10285760
	-- things not in elasoft but required for later version?
    "FieldIdList" varchar(1000),
    "StatsName" varchar(128),
    "StatsSource" char(1),
    "SampleSignature" varchar(256),
    "ThresholdSignature" varchar(512),
    "MaxIntervals" smallint,
    "MaxValueLength" int,
    "PNullUniqueValueCount" float8,
    "PNullHighModeFreq" float8,
    "StatsSkipCount" int,
    "LastCollectTimeStamp" Timestamp without Time Zone,
    "BLCCompRatio" int,
	"ObjectId" bytea	-- 6
);

-- elasoft
CREATE TABLE DBC.TextTbl (
    "TextId" bytea, -- 6
    "TextType" char(1),
    "LineNo" smallint,
    "TextString" varchar(32000),
    "DatabaseId" bytea -- 4
);

-- elasoft
CREATE VIEW DBC.DatabasesV
AS SELECT
          DBase."DatabaseName" AS "DatabaseName",
          DBase."CreatorName" AS "CreatorName",
          DBase."OwnerName" AS "OwnerName",
          DBase."AccountName" AS "AccountName",
          DBase."ProtectionType",
          DBase."JournalFlag",
          DBase."PermSpace",
          DBase."SpoolSpace",
          DBase."TempSpace",
          DBase."CommentString",
          DBase."CreateTimeStamp",
          DB2."DatabaseName" AS "LastAlterName",
          DBase."LastAlterTimeStamp",
          DBase."RowType" AS "DBKind",
          DBase."AccessCount",
          DBase."LastAccessTimeStamp"
FROM DBC.DBase
         LEFT OUTER JOIN DBC.Dbase DB2
                      ON DBC.DBase."LastAlterUID" = DB2."DatabaseId";

-- elasoft
CREATE VIEW DBC.TablesV
AS SELECT DBase."DatabaseName" AS "DataBaseName",   -- Inconsistent capitalization is correct.
          TVM."TVMName" AS "TableName",
          TVM."Version",
          TVM."TableKind",
          TVM."ProtectionType",
          TVM."JournalFlag",
          coalesce(DB1."DatabaseName", TVM."CreatorName") AS "CreatorName",
          TVM."RequestText",
          TVM."CommentString",
          TVM."ParentCount",
          TVM."ChildCount",
          TVM."NamedTblCheckCount",
          TVM."UnnamedTblCheckExist",
          TVM."PrimaryKeyIndexId",
          TVM."TblStatus" AS "RepStatus",
          TVM."CreateTimeStamp",
          DB2."DatabaseName" AS "LastAlterName",
          TVM."LastAlterTimeStamp",
          TVM."RequestTxtOverflow",
          TVM."AccessCount",
          TVM."LastAccessTimeStamp",
          TVM."UtilVersion",
          TVM."QueueFlag",
          TVM."CommitOpt",
          TVM."TransLog",
          TVM."CheckOpt"
FROM DBC.TVM
         LEFT OUTER JOIN DBC.Dbase DB1
                      ON DBC.TVM."CreatorName" = DB1."DatabaseNameI"
         LEFT OUTER JOIN DBC.Dbase DB2
                      ON DBC.TVM."LastAlterUID" = DB2."DatabaseId",
         DBC.DBase
WHERE TVM."DatabaseId" = DBase."DatabaseId"
  AND TVM."TVMId" NOT IN ( E'\\x00C001000000', E'\\x00C002000000',
                         E'\\x00C009000000', E'\\x00C010000000',
                         E'\\x00C017000000');

-- elasoft
CREATE VIEW DBC.TableTextV
AS SELECT
            DBase."DatabaseName" as "DataBaseName",
            TVM."TVMName" AS "TableName",
            TVM."TableKind",
            TextTbl."TextString" as "RequestText",
            TextTbl."LineNo"
FROM DBC.TVM,
     DBC.TextTbl,
     DBC.DBase
WHERE TVM."DatabaseId" = DBase."DatabaseId"
  AND TVM."TVMId" = TextTbl."TextId"
  AND TextTbl."TextType" = 'R';

-- elasoft
CREATE VIEW DBC.Columns
  ("DatabaseName","TableName","ColumnName","ColumnFormat","ColumnTitle",
   "SPParameterType","ColumnType","ColumnUDTName","ColumnLength","DefaultValue",
   "Nullable","CommentString","DecimalTotalDigits","DecimalFractionalDigits",
   "ColumnId","UpperCaseFlag","Compressible","CompressValue",
   "ColumnConstraint", "ConstraintCount",
   "CreatorName", "CreateTimeStamp", "LastAlterName", "LastAlterTimeStamp",
   "CharType", "IdColType", "AccessCount", "LastAccessTimeStamp",
   "CompressValueList")
AS
SELECT (CAST(DBase."DatabaseName" AS CHAR(30))),
       (CAST(TVM."TVMName" AS CHAR(30))),
       (CAST(tvfields."FieldName" AS CHAR(30))),
       (CAST(tvfields."FieldFormat" AS CHAR(30))),
       (CAST(tvfields."FieldTitle" AS VARCHAR(60))),
       tvfields."SPParameterType",
       tvfields."FieldType",
       (CAST(tvfields."UDTName" AS CHAR(30))),
       tvfields."MaxLength",
       tvfields."DefaultValue",
       tvfields."Nullable",
       tvfields."CommentString",
       tvfields."TotalDigits",
       tvfields."ImpliedPoint",
       tvfields."FieldId",
       tvfields."UpperCaseFlag",
       tvfields."Compressible",
       NULL,
       tvfields."ColumnCheck",
       tvfields."CheckCount",
       (CAST(DB1."DatabaseName" AS CHAR(30))),
       tvfields."CreateTimeStamp",
       (CAST(DB2."DatabaseName" AS CHAR(30))),
       tvfields."LastAlterTimeStamp",
       tvfields."CharType",
       tvfields."IdColType",
       tvfields."AccessCount",
       tvfields."LastAccessTimeStamp",
       tvfields."CompressValueList"
FROM DBC.tvfields
         LEFT OUTER JOIN DBC.Dbase DB1
                      ON DBC.tvfields."CreateUID" = DB1."DatabaseId"
         LEFT OUTER JOIN DBC.Dbase DB2
                      ON DBC.tvfields."LastAlterUID" = DB2."DatabaseId",
          DBC.Dbase, DBC.TVM
WHERE   TVM."DatabaseId" = DBase."DatabaseId"
        AND     TVM."TVMId" = tvfields."TableId";

-- elasoft
CREATE VIEW DBC.ColumnsV
  ("DatabaseName","TableName","ColumnName","ColumnFormat","ColumnTitle",
   "SPParameterType","ColumnType","ColumnUDTName","ColumnLength","DefaultValue",
   "Nullable","CommentString","DecimalTotalDigits","DecimalFractionalDigits",
   "ColumnId","UpperCaseFlag","Compressible","CompressValue",
   "ColumnConstraint", "ConstraintCount",
   "CreatorName", "CreateTimeStamp", "LastAlterName", "LastAlterTimeStamp",
   "CharType", "IdColType", "AccessCount", "LastAccessTimeStamp",
   "CompressValueList")
AS SELECT
	DBase."DatabaseName" AS "DatabaseName",
	TVM."TVMName" AS "TableName",
	TVFields."FieldName" AS "ColumnName",
	TVFields."FieldFormat" AS "ColumnFormat",
	tvfields."FieldTitle" AS "ColumnTitle",
	tvfields."SPParameterType" AS "SPParameterType",
	tvfields."FieldType" AS "ColumnType",
	tVFields."UDTName" AS "ColumnUDTName",
	tvfields."MaxLength" AS "ColumnLength",
	tvfields."DefaultValue",
	tvfields."Nullable",
	tvfields."CommentString",
	tvfields."TotalDigits" AS "DecimalTotalDigits",
	tvfields."ImpliedPoint" AS "DecimalFractionalDigits",
	tvfields."FieldId" AS "ColumnId",
	tvfields."UpperCaseFlag",
	tvfields."Compressible",
	NULL,
	tVFields."ColumnCheck" AS "ColumnConstraint",
	tvfields."CheckCount" AS "ConstraintCount",
	DB1."DatabaseName" AS "CreatorName",
	tvfields."CreateTimeStamp",
	DB2."DatabaseName" AS "LastAlterName",
	tvfields."LastAlterTimeStamp",
	tvfields."CharType",
	tvfields."IdColType",
	tvfields."AccessCount",
	tvfields."LastAccessTimeStamp",
	tvfields."CompressValueList"
FROM DBC.tvfields
         LEFT OUTER JOIN DBC.Dbase DB1
                      ON DBC.tvfields."CreateUID" = DB1."DatabaseId"
         LEFT OUTER JOIN DBC.Dbase DB2
                      ON DBC.tvfields."LastAlterUID" = DB2."DatabaseId",
          DBC.Dbase, DBC.TVM
WHERE TVM."DatabaseId" = DBase."DatabaseId"
       AND TVM."TVMId" = tvfields."TableId";

-- These are supposed to have same schema as ColumnsV
CREATE VIEW DBC.ColumnsQV AS SELECT * FROM DBC.ColumnsV;
CREATE VIEW DBC.ColumnsJQV AS SELECT * FROM DBC.ColumnsV;

-- elasoft
CREATE VIEW DBC.IndicesV
AS SELECT
	DBase."DatabaseName" AS "DatabaseName",
	TVM."TVMName" AS "TableName",
	indexes."IndexNumber",
	indexes."IndexType",
	indexes."UniqueFlag",
	Indexes."Name" AS "IndexName",
	TVFields."FieldName" AS "ColumnName",
	indexes."FieldPosition" AS "ColumnPosition",
	DB1."DatabaseName" AS "CreatorName",
	indexes."CreateTimeStamp",
	DB2."DatabaseName" AS "LastAlterName",
	indexes."LastAlterTimeStamp",
	indexes."IndexMode",
	indexes."AccessCount",
	indexes."LastAccessTimeStamp"
FROM DBC.indexes
         LEFT OUTER JOIN DBC.Dbase DB1
                      ON DBC.indexes."CreateUID" = DB1."DatabaseId"
         LEFT OUTER JOIN DBC.Dbase DB2
                      ON DBC.indexes."LastAlterUID" = DB2."DatabaseId",
         DBC.DBase, DBC.TVM, DBC.TVfields
WHERE   TVM."DatabaseId" = DBase."DatabaseId"
AND     TVM."TVMId" = indexes."TableId"
AND     TVM."TVMId" = tvfields."TableId"
AND     indexes."IndexType" NOT IN ('M','D')
AND     tvfields."FieldId" = indexes."FieldId";

-- elasoft
CREATE VIEW DBC.FunctionsV
AS SELECT
       DBase."DatabaseName" AS "DatabaseName",
       UDFInfo."FunctionName" AS "FunctionName",
       TVM."TVMName" AS "SpecificName",
       UDFInfo."FunctionId",
       UDFInfo."NumParameters",
       UDFInfo."ParameterDataTypes",
       UDFInfo."FunctionType",
       UDFInfo."ExternalName",
       UDFInfo."SrcFileLanguage",
       UDFInfo."NoSQLDataAccess",
       UDFInfo."ParameterStyle",
       UDFInfo."DeterministicOpt",
       UDFInfo."NullCall",
       UDFInfo."PrepareCount",
       UDFInfo."ExecProtectionMode",
       UDFInfo."ExtFileReference",
       UDFInfo."CharacterType",
       UDFInfo."Platform",
       UDFInfo."InterimFldSize",
       UDFInfo."RoutineKind",
       UDFInfo."ParameterUDTIds",
       TVM."AuthIdUsed",
       UDFInfo."MaxOutParameters",
       UDFInfo."GLOPSetDatabaseName",
       UDFInfo."GLOPSetMemberName"
FROM   DBC.UDFInfo, DBC.DBase, DBC.TVM
WHERE  DBC.UDFInfo."DatabaseId" = DBC.DBase."DatabaseId"
AND    DBC.UDFInfo."FunctionId" = DBC.TVM."TVMId"
AND    (UDFInfo."FunctionType" = 'F' OR
        UDFInfo."FunctionType" = 'A' OR
        UDFInfo."FunctionType" = 'S' OR
        UDFInfo."FunctionType" = 'B' OR
        UDFInfo."FunctionType" = 'R');

-- elasoft
CREATE VIEW DBC.TableSizeV
AS SELECT
         DataBaseSpace."Vproc",
         Dbase."DatabaseName" AS "DataBaseName",
         Dbase."AccountName" AS "AccountName",
         TVM."TVMName" AS "TableName",
         DataBaseSpace."CurrentPermSpace" AS "CurrentPerm",
         DataBaseSpace."PeakPermSpace" AS "PeakPerm"
FROM  DBC.Dbase, DBC.DataBaseSpace, DBC.TVM
WHERE DataBaseSpace."TableId" <> E'\\x000000000000'
 AND  DataBaseSpace."TableId" = TVM."TVMId"
 AND  TVM."DatabaseId" = Dbase."DatabaseId"
 AND  TVM."TableKind" NOT IN ('G','M','V');

CREATE VIEW DBC.StatsV AS
 SELECT DBC.DBase."DatabaseName" AS "DatabaseName"
        ,DBC.TVM."TVMName" AS "TableName"
        ,DBC.StatsTbl."ExpressionList" AS "ColumnName"
        ,DBC.StatsTbl."FieldIdList"
        ,DBC.StatsTbl."StatsName"
        ,DBC.StatsTbl."ExpressionCount"
        ,DBC.StatsTbl."StatsId"
        ,DBC.StatsTbl."StatsType"
        ,DBC.StatsTbl."StatsSource"
        ,DBC.StatsTbl."ValidStats"
        ,DBC.StatsTbl."DBSVersion"
        ,DBC.StatsTbl."IndexNumber"
        ,COALESCE(DBC.StatsTbl."SampleSignature"
                 ,'Global Default') AS "SampleSignature"
        ,DBC.StatsTbl."SampleSizePct"
        ,COALESCE(DBC.StatsTbl."ThresholdSignature"
                 ,'Global Default') AS "ThresholdSignature"
        ,DBC.StatsTbl."MaxIntervals"
        ,DBC.StatsTbl."MaxValueLength"
        ,DBC.StatsTbl."RowCount"
        ,DBC.StatsTbl."UniqueValueCount"
        ,DBC.StatsTbl."PNullUniqueValueCount"
        ,DBC.StatsTbl."NullCount"
        ,DBC.StatsTbl."AllNullCount"
        ,DBC.StatsTbl."HighModeFreq"
        ,DBC.StatsTbl."PNullHighModeFreq"
        ,DBC.StatsTbl."StatsSkipCount"
        ,DBC.StatsTbl."CreateTimeStamp"
        ,COALESCE(DBC.StatsTbl."LastCollectTimeStamp"
                 ,DBC.StatsTbl."CreateTimeStamp") AS "LastCollectTimeStamp"
        ,COALESCE(DBC.StatsTbl."LastAlterTimeStamp"
                 ,DBC.StatsTbl."CreateTimeStamp") AS "LastAlterTimeStamp"
        ,DBC.StatsTbl."BLCCompRatio"

 FROM DBC.StatsTbl
         ,DBC.Dbase
         ,DBC.TVM

 WHERE DBC.StatsTbl."DatabaseId" = DBC.DBASE."DatabaseId"
 AND DBC.StatsTbl."ObjectId" = DBC.TVM."TVMId"
 AND DBC.StatsTbl."StatsType" IN
                 ('T' /*Tables*/
                , 'I' /*Join Index*/
                , 'N' /*HashIndex*/
                , 'B' /*Base Temporary Table*/
                , 'V') /*View Stats*/
 AND DBC.TVM."TableKind" IN
                 ('T' /*Perm and Base Temp Tables*/
                , 'I' /*Join Index*/
                , 'N' /*HashIndex*/
                , 'O' /*NOPI Table*/
                , 'Q' /*Queue Table*/
                , 'V') /*View Stats*/
 UNION ALL
 /*For Materialized temp tables*/
 SELECT DBC.DBase."DatabaseName" AS "DatabaseName"
        ,DBC.TVM."TVMName" AS "TableName"
        ,DBC.StatsTbl."ExpressionList" AS "ColumnName"
        ,DBC.StatsTbl."FieldIdList"
        ,DBC.StatsTbl."StatsName"
        ,DBC.StatsTbl."ExpressionCount"
        ,DBC.StatsTbl."StatsId"
        ,DBC.StatsTbl."StatsType"
        ,DBC.StatsTbl."StatsSource"
        ,DBC.StatsTbl."ValidStats"
        ,DBC.StatsTbl."DBSVersion"
        ,DBC.StatsTbl."IndexNumber"
        ,COALESCE(DBC.StatsTbl."SampleSignature"
                 ,'Global Default') AS "SampleSignature"
        ,DBC.StatsTbl."SampleSizePct"
        ,COALESCE(DBC.StatsTbl."ThresholdSignature"
                 ,'Global Default') AS "ThresholdSignature"
        ,DBC.StatsTbl."MaxIntervals"
        ,DBC.StatsTbl."MaxValueLength"
        ,DBC.StatsTbl."RowCount"
        ,DBC.StatsTbl."UniqueValueCount"
        ,DBC.StatsTbl."PNullUniqueValueCount"
        ,DBC.StatsTbl."NullCount"
        ,DBC.StatsTbl."AllNullCount"
        ,DBC.StatsTbl."HighModeFreq"
        ,DBC.StatsTbl."PNullHighModeFreq"
        ,DBC.StatsTbl."StatsSkipCount"
        ,DBC.StatsTbl."CreateTimeStamp"
        ,COALESCE(DBC.StatsTbl."LastCollectTimeStamp"
                 ,DBC.StatsTbl."CreateTimeStamp") AS "LastCollectTimeStamp"
        ,COALESCE(DBC.StatsTbl."LastAlterTimeStamp"
                 ,DBC.StatsTbl."CreateTimeStamp") AS "LastAlterTimeStamp"
        ,DBC.StatsTbl."BLCCompRatio"

 FROM DBC.StatsTbl
         ,DBC.TempTables
         ,DBC.Dbase
         ,DBC.TVM

 WHERE DBC.StatsTbl."DatabaseId" = DBC.DBASE."DatabaseId"
 AND DBC.StatsTbl."ObjectId" = DBC.TempTables."TableId"
 AND DBC.TempTables."BaseTableId" = TVM."TVMId"
 -- AND DBC.TempTables.SessionNo = SESSION /*Only show tables for the current session*/
 AND DBC.StatsTbl."StatsType" = 'M' /*Materialized Temp Tables*/
 AND (DBC.TVM."TableKind" ='T' /*Tables*/
           OR DBC.TVM."TableKind" ='O' /*NOPI Tables*/
           OR DBC.TVM."TableKind" ='Q') /*QUEUE Tables*/
 ;
