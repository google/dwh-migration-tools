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
    InfoKey VARCHAR(30),
    InfoData VARCHAR(16384)
);

CREATE VIEW DBC.DBCInfo AS
    SELECT
        InfoKey,
        CAST(SUBSTRING(InfoData FROM 1 FOR 16384) AS VARCHAR(16384)) AS InfoData
    FROM DBC.DBCInfoTbl;

CREATE VIEW DBC.DBCInfoV AS
    SELECT
        InfoKey,
        DBCInfoTbl.InfoData AS InfoData
    FROM DBC.DBCInfoTbl;

-- elasoft
CREATE TABLE DBC.DBase (
	DatabaseNameI varchar(256),
	DatabaseId bytea,	-- 4
	OwnerId bytea,	-- 4
	EncryptionFlag bytea,	-- 1
	PasswordSalt bytea,	-- 2
	EncryptedPassword bytea,	-- varbyte 512
	PasswordModTime Timestamp without Time Zone,
	ProtectionType char(1),
	JournalFlag char(2),
	PermSpace float8,
	SpoolSpace float8,
	StartupString varchar(510),
	CommentString varchar(510),
	AccountName varchar(256),
	CreatorName varchar(256),
	DatabaseName varchar(256),
	JournalId bytea,	-- 6
	Version smallint,
	OwnerName varchar(256),
	NumFallBackTables smallint,
	NumLogProtTables smallint,
	DefaultDataBase varchar(256),
	LogonRules smallint,
	AccLogRules smallint,
	AccLogUsrRules smallint,
	DefaultCollation char(1),
	RowType char(1),
	PasswordChgDate int,
	LockedDate int,
	LockedTime smallint,
	LockedCount char,	-- byteint
	UnResolvedRICount smallint,
	TimeZoneHour char,	-- byteint
	TimeZoneMinute char,	-- byteint
	DefaultDateForm char(1),
	CreateUID bytea,	-- 4
	CreateTimeStamp Timestamp without Time Zone,
	LastAlterUID bytea,	-- 4
	LastAlterTimeStamp Timestamp without Time Zone,
	TempSpace float8,
	LastAccessTimeStamp Timestamp without Time Zone,
	AccessCount int,
	DefaultCharType smallint,
	RoleName varchar(256),
	ProfileName varchar(256),
	UDFLibRevision int,
	AppCat1Revision int,
	AppCat2Revision int,
	AppCat3Revision int,
	AppCat4Revision int,
	JarLibRevision int,
	TimeZoneString varchar(256),
	DefaultMapNo int,	-- not in elasoft
	ZoneID bytea	-- probably 4
);

-- no idea
CREATE TABLE DBC.DatasetSchemaInfo (
	DatasetSchemaId int,
	DatasetSchemaName varchar(64)
);

-- elasoft
CREATE TABLE DBC.TVM (
	DatabaseId bytea,	-- 4
	TVMNameI varchar(256),
	LogicalHostId smallint,
	SessionNo int,
	TVMId bytea,	-- 6
	TableKind char(1),
	ProtectionType char(1),
	TempFlag char(1),
	HashFlag char(1),
	NextIndexId smallint,
	NextFieldId smallint,
	Version smallint,
	RequestText varchar(25000),
	CreateText varchar(26000),
	CommentString varchar(510),
	CreatorName varchar(256),
	TVMName varchar(256),
	JournalFlag char(2),
	JournalId bytea,	-- 6
	UtilVersion smallint,
	AccLogRules char(1),
	ColumnAccRules smallint,
	CheckOpt char(1),
	ParentCount smallint,
	ChildCount smallint,
	NamedTblCheckCount smallint,
	UnnamedTblCheckExist char(1),
	PrimaryKeyIndexId smallint,
	CreateUID bytea,	-- 4
	CreateTimeStamp Timestamp without Time Zone,
	LastAlterUID bytea,	-- 4
	LastAlterTimeStamp Timestamp without Time Zone,
	TriggerCount smallint,
	CommitOpt char(1),
	TransLog char(1),
	LastAccessTimeStamp Timestamp without Time Zone,
	AccessCount int,
	SPObjectCodeRows int,
	RSGroupID int,
	TblRole char(1),
	TblStatus char(1),
	RequestTxtOverflow char(1),
	CreateTxtOverflow char(1),
	QueueFlag char(1),
	XSPExternalName char(30),
	XSPOptions char(3),
	XSPExtFileReference varchar(2000),
	ExecProtectionMode char(1),
	CharacterType smallint,
	Platform char(8),
	AuthIdUsed bytea,	-- 6
	AuthorizationType char(1),
	AuthorizationSubType char(1),
	OSDomainName varchar(256),
	OSUserName varchar(256),
	SecInfo bytea,
	AuthName varchar(256),
	TemporalProperty char(1),
	ResolvedCurrent_Date date,
	ResolvedCurrent_Timestamp Timestamp with Time Zone,
	SystemDefinedJI char(1),
	VTQualifier char(1),
	TTQualifier char(1)
);

-- elasoft
create table DBC.tvfields (
	TableId bytea,	-- 6
	FieldName varchar(256),
	FieldId smallint,
	Nullable char(1),
	FieldType char(2),
	MaxLength int,
	DefaultValue varchar(2048),
	DefaultValueI bytea,	-- 1024
	TotalDigits smallint,
	ImpliedPoint smallint,
	FieldFormat varchar(256),
	FieldTitle varchar(512),
	CommentString varchar(510),
	CollationFlag char(1),
	UpperCaseFlag char(1),
	DatabaseId bytea,	-- 4
	Compressible char(1),
	CompressValueList varchar(16384),
	FieldStatistics bytea,	-- 16383
	ColumnCheck varchar(16384),
	CheckCount smallint,
	CreateUID bytea,	-- 4
	CreateTimeStamp Timestamp without Time Zone,
	LastAlterUID bytea,	-- 4
	LastAlterTimeStamp Timestamp without Time Zone,
	LastAccessTimeStamp Timestamp without Time Zone,
	AccessCount int,
	SPParameterType char(1),
	CharType smallint,
	LobSequenceNo smallint,
	IdColType char(2),
	UDTypeId bytea,	-- 6
	UDTName varchar(256),
	TimeDimension char(1),
	VTCheckType char(1),
	TTCheckType char(1),
	ConstraintId bytea,	-- 4
	DatasetSchemaId int,	-- not in elasoft
	PartitioningColumn char(1),	-- not in elasoft
	ColumnPartitionNumber int8,	-- not in elasoft
	ColumnPartitionFormat char(2),	-- not in elasoft
	ColumnPartitionAC char(2),	-- not in elasoft
	PseudoUDTFieldId smallint,	-- not in elasoft
	PseudoUDTFieldType char(2),	-- not in elasoft
	InlineLength int	-- not in anything
);

-- elasoft
CREATE TABLE DBC.UDTInfo (
	TypeId bytea,	-- 6
	DatabaseId bytea,	-- 4
	TypeName varchar(256),
	TypeKind char(1),
	INSTANTIABLE char(1),
	FINAL char(1),
	Encryption char(1),
	Compression char(1),
	OperatorAll char(1),
	DefaultTransformGroup varchar(256),
	OrderingForm char(1),
	OrderingCategory char(1),
	OrderingRoutineId bytea,	-- 6
	CastCount char,	-- byteint
	ExtFileReference varchar(2000),
	ArrayScope varchar(32000),	-- Not in elasoft
	ArrayNumDimensions char	-- Not in elasoft
);

-- No idea
CREATE TABLE DBC.ObjectUsage (
	DatabaseId bytea,	-- 4
	ObjectId bytea,	-- probably 6
	FieldId smallint,
	IndexNumber int,	-- ?
	UsageType varchar(16),
	UserAccessCnt int,	-- ?
	LastAccessTimeStamp Timestamp without Time Zone
);

-- No idea
CREATE TABLE DBC.Maps (
	MapNo	int,
	MapName	varchar(64)
);

CREATE VIEW DBC.DatabasesV
 AS SELECT
           DBase.DatabaseName AS DatabaseName,
           DBase.CreatorName AS CreatorName,
           DBase.OwnerName AS OwnerName,
           DBase.AccountName AS AccountName,
           DBase.ProtectionType,
           DBase.JournalFlag,
           DBase.PermSpace,
           DBase.SpoolSpace,
           DBase.TempSpace,
           DBase.CommentString,
           DBase.CreateTimeStamp,
           DB2.DatabaseName AS LastAlterName,
           DBase.LastAlterTimeStamp,
           DBase.RowType AS DBKind,
           OU.UserAccessCnt AS AccessCount,
           OU.LastAccessTimeStamp,
           Maps.MapName
 FROM DBC.DBase
          LEFT OUTER JOIN DBC.Dbase DB2
                       ON DBC.DBase.LastAlterUID = DB2.DatabaseID
          LEFT OUTER JOIN DBC.ObjectUsage OU
                       ON OU.DatabaseId = DBC.Dbase.DatabaseId
                      AND OU.ObjectId IS NULL
          LEFT OUTER JOIN DBC.Maps
                       ON DBC.Dbase.DefaultMapNo = DBC.Maps.MapNo
 WHERE (DBC.DBase.DatabaseId = E'\\x00000000'
    OR DBC.DBase.DatabaseId <> DBC.DBase.ZoneID);

CREATE VIEW DBC.Columns
   (DatabaseName,TableName,ColumnName,ColumnFormat,ColumnTitle,
    SPParameterType,ColumnType,ColumnUDTName,ColumnLength,DefaultValue,
    Nullable,CommentString,DecimalTotalDigits,DecimalFractionalDigits,
    ColumnId,UpperCaseFlag,Compressible,CompressValue,
    ColumnConstraint, ConstraintCount,
    CreatorName, CreateTimeStamp, LastAlterName, LastAlterTimeStamp,
    CharType, IdColType, AccessCount, LastAccessTimeStamp,
    CompressValueList, TimeDimension, VTCheckType, TTCheckType, ConstraintId,
    ArrayColNumberOfDimensions, ArrayColScope, ArrayColElementType, ArrayColElementUdtName)
 AS
 SELECT (CAST(SUBSTRING(dbase.DatabaseName FROM 1 FOR 30) AS CHAR(30))),
        (CAST(SUBSTRING(tvm.TVMName FROM 1 FOR 30) AS CHAR(30))),
        (CAST(SUBSTRING(tvfields.FieldName FROM 1 FOR 30) AS CHAR(30))),
        (CAST(SUBSTRING(tvfields.FieldFormat FROM 1 FOR 30) AS CHAR(30))),
        (CAST(SUBSTRING(tvfields.FieldTitle FROM 1 FOR 60) AS VARCHAR(60))),
        tvfields.SPParameterType,
        tvfields.FieldType,
        (CAST(SUBSTRING(tvfields.UDTName FROM 1 FOR 30) AS CHAR(30))),
        tvfields.MaxLength,
        tvfields.DefaultValue,
        tvfields.Nullable,
        tvfields.CommentString,
        tvfields.TotalDigits,
        tvfields.ImpliedPoint,
        tvfields.FieldId,
        tvfields.UpperCaseFlag,
        tvfields.Compressible,
        NULL,
        tvfields.ColumnCheck,
        tvfields.CheckCount,
        (CAST(SUBSTRING(DB1.DatabaseName FROM 1 FOR 30) AS CHAR(30))),
        tvfields.CreateTimeStamp,
        (CAST(SUBSTRING(DB2.DatabaseName FROM 1 FOR 30) AS CHAR(30))),
        tvfields.LastAlterTimeStamp,
        tvfields.CharType,
        tvfields.IdColType,
        OU.UserAccessCnt,
        OU.LastAccessTimeStamp,
        tvfields.CompressValueList,
        tvfields.TimeDimension,
        tvfields.VTCheckType,
        tvfields.TTCheckType,
        tvfields.ConstraintId,
        udt1.ArrayNumDimensions AS ArrayColNumberOfDimensions,
        udt1.ArrayScope AS ArrayColScope,
        tvf2.FieldType AS ArrayColElementType,
        CAST(SUBSTRING(tvf2.UDTName FROM 1 FOR 30) AS CHAR(30)) AS ArrayColElementUdtName
 FROM DBC.tvfields
          LEFT OUTER JOIN DBC.ObjectUsage OU
                       ON OU.DatabaseId = DBC.TVFields.DatabaseId
                      AND OU.ObjectId = DBC.TVFields.TableId
                      AND OU.FieldId = DBC.TVFields.FieldId
                      AND OU.IndexNumber IS NULL
                      AND OU.UsageType = 'DML'
          LEFT OUTER JOIN DBC.Dbase DB1
                       ON DBC.tvfields.CreateUID = DB1.DatabaseID
          LEFT OUTER JOIN DBC.Dbase DB2
                       ON DBC.tvfields.LastAlterUID = DB2.DatabaseID
          LEFT OUTER JOIN DBC.tvfields tvf2
                       ON DBC.tvfields.UDTypeId=tvf2.TableId AND
                          tvf2.FieldName = 'ARRAYELEMENT' AND
            (DBC.tvfields.FieldType='A1' OR
                          DBC.tvfields.FieldType='AN')
          LEFT OUTER JOIN DBC.udtinfo udt1
                       ON DBC.tvfields.UDTypeId=udt1.TypeId AND (DBC.tvfields.FieldType='A1' OR
                                                                 DBC.tvfields.FieldType='AN'),
           DBC.Dbase, DBC.TVM
 WHERE tvm.DatabaseId = dbase.DatabaseId
         AND tvm.tvmid = tvfields.tableid;

CREATE VIEW DBC.ColumnsV
   (DatabaseName,TableName,ColumnName,ColumnFormat,ColumnTitle,
    SPParameterType,ColumnType,ColumnUDTName,ColumnLength,DefaultValue,
    Nullable,CommentString,DecimalTotalDigits,DecimalFractionalDigits,
    ColumnId,UpperCaseFlag,Compressible,CompressValue,
    ColumnConstraint, ConstraintCount,
    CreatorName, CreateTimeStamp, LastAlterName, LastAlterTimeStamp,
    CharType, IdColType, AccessCount, LastAccessTimeStamp,
    CompressValueList, TimeDimension, VTCheckType, TTCheckType, ConstraintId,
    ArrayColNumberOfDimensions, ArrayColScope, ArrayColElementType, ArrayColElementUdtName,
    PartitioningColumn, ColumnPartitionNumber, ColumnPartitionFormat,
    ColumnPartitionAC, PseudoUDTFieldId, PseudoUDTFieldType, StorageFormat, DatasetSchemaName, InlineLength)
 AS SELECT
     DBase.DatabaseName AS DatabaseName,
     TVM.TVMName AS TableName,
     TVFields.FieldName AS ColumnName,
     TVFields.FieldFormat AS ColumnFormat,
     tvfields.fieldtitle AS ColumnTitle,
     tvfields.SPParameterType AS SPParameterType,
     tvfields.fieldtype AS ColumnType,
     tVFields.UDTName AS ColumnUDTName,
     tvfields.maxlength AS ColumnLength,
     tvfields.DefaultValue,
     tvfields.Nullable,
     tvfields.CommentString,
     tvfields.TotalDigits AS DecimalTotalDigits,
     tvfields.ImpliedPoint AS DecimalFractionalDigits,
     tvfields.FieldId AS ColumnId,
     tvfields.UpperCaseFlag,
     tvfields.Compressible,
     NULL,
     tVFields.ColumnCheck AS ColumnConstraint,
     tvfields.CheckCount AS ConstraintCount,
     DB1.DatabaseName AS CreatorName,
     tvfields.CreateTimeStamp,
     DB2.DatabaseName AS LastAlterName,
     tvfields.LastAlterTimeStamp,
     tvfields.CharType,
     tvfields.IdColType,
     OU.UserAccessCnt,
     OU.LastAccessTimeStamp,
     tvfields.CompressValueList,
     tvfields.TimeDimension,
     tvfields.VTCheckType,
     tvfields.TTCheckType,
     tvfields.ConstraintId,
     udt1.ArrayNumDimensions AS ArrayColNumberOfDimensions,
     udt1.ArrayScope AS ArrayColScope,
     tvf2.FieldType AS ArrayColElementType,
     tvf2.UDTName AS ArrayColElementUdtName,
     tvfields.PartitioningColumn,
     tvfields.ColumnPartitionNumber,
     tvfields.ColumnPartitionFormat,
     tvfields.ColumnPartitionAC,
     tvfields.PseudoUDTFieldId,
     tvfields.PseudoUDTFieldType,
     (CASE WHEN tvfields.FieldType = 'JN'
             THEN
                 (CASE WHEN tvfields.UDTypeId = E'\\x00C059000000' THEN 'BSON'
                       WHEN tvfields.UDTypeId = E'\\x00C05D000000' THEN 'BSON'
                       WHEN tvfields.UDTypeId = E'\\x00C05A000000' THEN 'UBJSON'
                       WHEN tvfields.UDTypeId = E'\\x00C05E000000' THEN 'UBJSON'
                       ELSE 'TEXT'
                  END)
           WHEN tvfields.FieldType = 'DT'
             THEN
                 (CASE WHEN tvfields.UDTypeId = E'\\x00C05F000000' THEN 'AVRO'
                       WHEN tvfields.UDTypeId = E'\\x00C060000000' THEN 'AVRO'
                       ELSE NULL
                  END)
             ELSE NULL
     END) AS StorageFormat,
     (CASE WHEN tvfields.datasetschemaid IS NOT NULL
             THEN dsc.DatasetSchemaName
             ELSE NULL
     END) AS DatasetSchemaName,
     tvfields.InlineLength
 FROM DBC.tvfields
          LEFT OUTER JOIN DBC.ObjectUsage OU
                       ON OU.DatabaseId = DBC.TVFields.DatabaseId
                      AND OU.ObjectId = DBC.TVFields.TableId
                      AND OU.FieldId = DBC.TVFields.FieldId
                      AND OU.IndexNumber IS NULL
                      AND OU.UsageType = 'DML'
          LEFT OUTER JOIN DBC.Dbase DB1
                       ON DBC.tvfields.CreateUID = DB1.DatabaseID
          LEFT OUTER JOIN DBC.Dbase DB2
                       ON DBC.tvfields.LastAlterUID = DB2.DatabaseID
          LEFT OUTER JOIN DBC.tvfields tvf2
                       ON DBC.tvfields.UDTypeId=tvf2.TableId AND
            tvf2.FieldName = 'ARRAYELEMENT' AND
            (DBC.tvfields.FieldType='A1' OR
                          DBC.tvfields.FieldType='AN')
          LEFT OUTER JOIN DBC.udtinfo udt1
                       ON DBC.tvfields.UDTypeId=udt1.TypeId AND (DBC.tvfields.FieldType='A1' OR
                                                                 DBC.tvfields.FieldType='AN')
          LEFT OUTER JOIN DBC.DatasetSchemaInfo dsc
                       ON DBC.tvfields.datasetschemaid=dsc.datasetschemaid AND (DBC.tvfields.FieldType='DT'),
      DBC.dbase, DBC.TVM
 WHERE tvm.DatabaseId = dbase.DatabaseId;

