/*
 * Copyright 2022 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.edwmigration.dumper.plugin.lib.dumper.spi;

/**
 *
 * @author shevek
 */
public interface TeradataMetadataDumpFormat {

    public static final String FORMAT_NAME = "teradata.dump.zip";

    public static interface VersionFormat {

        public static final String ZIP_ENTRY_NAME = "version.csv";
    }

    public static interface ColumnsFormat {

        public static final String ZIP_ENTRY_NAME = "dbc.Columns.csv";

        // Complete Set for _some_ TD from Issue:5673
        //  DatabaseName,TableName,ColumnName,ColumnFormat,ColumnTitle,SPParameterType,
        //  ColumnType,ColumnUDTName,ColumnLength,DefaultValue,Nullable,CommentString,
        //  DecimalTotalDigits,DecimalFractionalDigits,ColumnId,UpperCaseFlag,Compressible,
        //  CompressValue,ColumnConstraint,ConstraintCount,CreatorName,CreateTimeStamp,
        //  LastAlterName,LastAlterTimeStamp,CharType,IdColType,AccessCount,LastAccessTimeStamp,
        //  CompressValueList,TimeDimension,VTCheckType,TTCheckType,ConstraintId,ArrayColNumberOfDimensions,
        //  ArrayColScope,ArrayColElementType,ArrayColElementUdtName,PartitioningColumn,ColumnPartitionNumber,
        //  ColumnPartitionFormat,ColumnPartitionAC,PseudoUDTFieldId,PseudoUDTFieldType,StorageFormat,DatasetSchemaName,InlineLength,TSColumnType
        // NOTE: This enum must match not what TD says is in the schema, but what we actually dump.
        public static enum Header {
            DatabaseName, TableName, /** New. */
            ColumnId, ColumnName, ColumnType;
        }
    }

    public static interface DatabasesVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.DatabasesV.csv";

        // TD 16.20.32.09 - complete set.
        public static enum Header {
            DatabaseName, CreatorName, OwnerName, AccountName, ProtectionType, JournalFlag, PermSpace, SpoolSpace, TempSpace, CommentString, CreateTimeStamp, LastAlterName, LastAlterTimeStamp, DBKind, AccessCount, LastAccessTimeStamp,
            /** Not in earlier versions. */
            DefaultMapName, MapOverride
        }
    }

    public static interface TablesVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.TablesV.csv";

        // TD 16.20.32.09 - complete set.
        public static enum Header {
            DataBaseName, TableName, Version, TableKind, ProtectionType, JournalFlag, CreatorName, RequestText, CommentString, ParentCount, ChildCount, NamedTblCheckCount, UnnamedTblCheckExist, PrimaryKeyIndexId, RepStatus, CreateTimeStamp, LastAlterName, LastAlterTimeStamp, RequestTxtOverflow, AccessCount, LastAccessTimeStamp, UtilVersion, QueueFlag, CommitOpt, TransLog, CheckOpt, TemporalProperty, ResolvedCurrent_Date, ResolvedCurrent_Timestamp, SystemDefinedJI, VTQualifier, TTQualifier, PIColumnCount, PartitioningLevels, LoadProperty, CurrentLoadId, LoadIdLayout, DelayedJI, LastArchiveId, LastFullArchiveId, BlockSize, FreeSpacePercent, MergeBlockRatio, CheckSum, BlockCompression, BlockCompressionAlgorithm, BlockCompressionLevel, TableHeaderFormat, RowSizeFormat, MapName, ColocationName, TVMFlavor
        }
    }

    public static interface TableTextVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.TableTextV.csv";

        public static enum Header {
            DataBaseName, TableName, TableKind, RequestText, LineNo
        }
    }

    public static interface IndicesVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.IndicesV.csv";

        // TD 16.20.32.09 - complete set.
        public static enum Header {
            DatabaseName, TableName, IndexNumber, IndexType, UniqueFlag, IndexName, ColumnName, ColumnPosition, CreatorName, CreateTimeStamp, LastAlterName, LastAlterTimeStamp, IndexMode, AccessCount, LastAccessTimeStamp, UniqueOrPK, VTConstraintType, TTConstraintType, SystemDefinedJI, IndexDatabaseName, LDIType, RowSizeFormat, TimeZero, TimeBucketUnit, TimeBucketValue, TSFlags
        }
    }

    public static interface PartitioningConstraintsVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.PartitioningConstraintsV.csv";

        // TD 16.20.32.09 - complete set.
        public static enum Header {
            DatabaseName, TableName, IndexName, IndexNumber, ConstraintType, ConstraintText, ConstraintCollation, CollationName, CreatorName, CreateTimeStamp, CharSetID, SessionMode, ResolvedCurrent_Date, ResolvedCurrent_TimeStamp, DefinedCombinedPartitions, MaxCombinedPartitions, PartitioningLevels, ColumnPartitioningLevel
        }
    }

    public static interface ColumnsVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.ColumnsV.csv";

        // TD 16.20.32.09 - complete set.
        public static enum Header {
            DatabaseName, TableName, ColumnName, ColumnFormat, ColumnTitle, SPParameterType, ColumnType, ColumnUDTName, ColumnLength, DefaultValue, Nullable, CommentString, DecimalTotalDigits, DecimalFractionalDigits, ColumnId, UpperCaseFlag, Compressible, CompressValue, ColumnConstraint, ConstraintCount, CreatorName, CreateTimeStamp, LastAlterName, LastAlterTimeStamp, CharType, IdColType, AccessCount, LastAccessTimeStamp, CompressValueList, TimeDimension, VTCheckType, TTCheckType, ConstraintId, ArrayColNumberOfDimensions, ArrayColScope, ArrayColElementType, ArrayColElementUdtName, PartitioningColumn, ColumnPartitionNumber, ColumnPartitionFormat, ColumnPartitionAC, PseudoUDTFieldId, PseudoUDTFieldType, StorageFormat, DatasetSchemaName, InlineLength, TSColumnType
        }
    }

    public static interface ColumnsQVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.ColumnsQV.csv";
    }

    public static interface ColumnsJQVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.ColumnsJQV.csv";
    }

    public static interface FunctionsVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.FunctionsV.csv";

        // TD 16.20.32.09 - complete set.
        public static enum Header {
            DatabaseName, FunctionName, SpecificName, FunctionId, NumParameters, ParameterDataTypes, FunctionType, ExternalName, SrcFileLanguage, NoSQLDataAccess, ParameterStyle, DeterministicOpt, NullCall, PrepareCount, ExecProtectionMode, ExtFileReference, CharacterType, Platform, InterimFldSize, RoutineKind, ParameterUDTIds, AuthIdUsed, MaxOutParameters, GLOPSetDatabaseName, GLOPSetMemberName, RefQueryband, ExecMapName, ExecMapColocName
        }
    }

    public static interface StatsVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.StatsV.csv";

        // TD 16.20.32.09 - complete set.
        public static enum Header {
            DatabaseName, TableName, ColumnName, FieldIdList, StatsName, ExpressionCount, StatsId, StatsType, StatsSource, ValidStats, DBSVersion, IndexNumber, SampleSignature, SampleSizePct, ThresholdSignature, MaxIntervals, MaxValueLength, RowCount, UniqueValueCount, PNullUniqueValueCount, NullCount, AllNullCount, HighModeFreq, PNullHighModeFreq, StatsSkipCount, CreateTimeStamp, LastCollectTimeStamp, LastAlterTimeStamp, BLCCompRatio
        }
    }

    public static interface TableSizeVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.TableSizeV.csv";

        // TD 16.20.32.09 - complete set.
        public static enum Header {
            Vproc, DataBaseName, AccountName, TableName, CurrentPerm, PeakPerm
        }
    }

    public static interface AllTempTablesVXFormat {

        public static String ZIP_ENTRY_NAME = "dbc.AllTempTablesVX.csv";

        public static enum Header {
            HostNo, SessionNo, UserName, B_DatabaseName, B_TableName, E_TableId
        }
    }

    public static interface DiskSpaceVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.DiskSpaceV.csv";

        public static enum Header {
            VProc, DatabaseName, AccountName, MaxPerm, MaxSpool, MaxTemp, CurrentPerm, CurrentSpool, CurrentPersistentSpool, CurrentTemp, PeakPerm, PeakSpool, PeakPersistentSpool, PeakTemp, MaxProfileSpool, MaxProfileTemp, TrustUserName, AppProxyUser, AllocatedPerm, AllocatedSpool, AllocatedTemp, PermSkew, SpoolSkew, TempSkew
        }
    }

    public static interface RoleMembersVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.RoleMembersV.csv";

        public static enum Header {
            RoleName, Grantor, Grantee, WhenGranted, DefaultRole, WithAdmin
        }
    }

    public static interface All_RI_ChildrenVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.All_RI_ChildrenV.csv";

        public static enum Header {
            IndexId, IndexName, ChildDB, ChildTable, ChildKeyColumn, ParentDB, ParentTable, ParentKeyColumn, InconsistencyFlag, CreatorName, CreateTimeStamp
        }
    }

    public static interface All_RI_ParentsVFormat {

        public static String ZIP_ENTRY_NAME = "dbc.All_RI_ParentsV.csv";

        public static enum Header {
            IndexId, IndexName, ChildDB, ChildTable, ChildKeyColumn, ParentDB, ParentTable, ParentKeyColumn, InconsistencyFlag, CreatorName, CreateTimeStamp
        }
    }
}
