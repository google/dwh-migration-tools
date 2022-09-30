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
 * @author matt
 */
public interface TeradataLogsDumpFormat {

    String FORMAT_NAME = "teradata.logs.zip";  // Has Header or (HeaderLSql+HeaderLog )
    String ZIP_ENTRY_PREFIX = "query_history_";

    String ZIP_ENTRY_PREFIX_LSQL = "query_history_lsql_";
    String ZIP_ENTRY_PREFIX_LOG = "query_history_log_";

    enum Header {
        QueryID,
        SQLRowNo, // 1,2,... All SQLTextInfo to be concated based on this.
        SQLTextInfo,
        UserName,
        CollectTimeStamp,
        StatementType,
        AppID,
        DefaultDatabase,
        ErrorCode,
        ErrorText,
        FirstRespTime,
        LastRespTime,
        NumResultRows,
        QueryText,
        ReqPhysIO,
        ReqPhysIOKB,
        RequestMode,
        SessionID,
        SessionWDID,
        Statements,
        TotalIOCount,
        WarningOnly

    }

    enum HeaderForAssessment {
        QueryID,
        SQLRowNo,
        SQLTextInfo,
        AbortFlag,
        AcctString,
        AcctStringDate,
        AcctStringHour,
        AcctStringTime,
        AMPCPUTime,
        AMPCPUTimeNorm,
        AppID,
        CacheFlag,
        CalendarName,
        CallNestingLevel,
        CheckpointNum,
        ClientAddr,
        ClientID,
        CollectTimeStamp,
        CPUDecayLevel,
        DataCollectAlg,
        DBQLStatus,
        DefaultDatabase,
        DelayTime,
        DisCPUTime,
        DisCPUTimeNorm,
        ErrorCode,
        EstMaxRowCount,
        EstMaxStepTime,
        EstProcTime,
        EstResultRows,
        ExpandAcctString,
        FirstRespTime,
        FirstStepTime,
        FlexThrottle,
        ImpactSpool,
        InternalRequestNum,
        IODecayLevel,
        IterationCount,
        KeepFlag,
        LastRespTime,
        LockDelay,
        LockLevel,
        LogicalHostID,
        LogonDateTime,
        LogonSource,
        LSN,
        MaxAMPCPUTime,
        MaxAMPCPUTimeNorm,
        MaxAmpIO,
        MaxCPUAmpNumber,
        MaxCPUAmpNumberNorm,
        MaxIOAmpNumber,
        MaxNumMapAMPs,
        MaxOneMBRowSize,
        MaxStepMemory,
        MaxStepsInPar,
        MinAmpCPUTime,
        MinAmpCPUTimeNorm,
        MinAmpIO,
        MinNumMapAMPs,
        MinRespHoldTime,
        NumFragments,
        NumOfActiveAMPs,
        NumRequestCtx,
        NumResultOneMBRows,
        NumResultRows,
        NumSteps,
        NumStepswPar,
        ParamQuery,
        ParserCPUTime,
        ParserCPUTimeNorm,
        ParserExpReq,
        PersistentSpool,
        ProcID,
        ProfileID,
        ProfileName,
        ProxyRole,
        ProxyUser,
        ProxyUserID,
        QueryBand,
        QueryRedriven,
        QueryText,
        ReDriveKind,
        RemoteQuery,
        ReqIOKB,
        ReqPhysIO,
        ReqPhysIOKB,
        RequestMode,
        RequestNum,
        SeqRespTime,
        SessionID,
        SessionTemporalQualifier,
        SpoolUsage,
        StartTime,
        StatementGroup,
        Statements,
        StatementType,
        SysDefNumMapAMPs,
        TacticalCPUException,
        TacticalIOException,
        TDWMEstMemUsage,
        ThrottleBypassed,
        TotalFirstRespTime,
        TotalIOCount,
        TotalServerByteCount,
        TTGranularity,
        TxnMode,
        TxnUniq,
        UnitySQL,
        UnityTime,
        UsedIota,
        UserID,
        UserName,
        UtilityByteCount,
        UtilityInfoAvailable,
        UtilityRowCount,
        VHLogicalIO,
        VHLogicalIOKB,
        VHPhysIO,
        VHPhysIOKB,
        WarningOnly,
        WDName
    }

    enum HeaderLSql {
        QueryID,
        CollectTimeStamp,
        SQLRowNo, // 1,2,... All SQLTextInfo to be concated based on this.
        SQLTextInfo
    }

    enum HeaderLog {
        CollectTimeStamp,
        UserName,
        StatementType,
        AppID,
        DefaultDatabase,
        ErrorCode,
        ErrorText,
        FirstRespTime,
        LastRespTime,
        NumResultRows,
        QueryText,
        ReqPhysIO,
        ReqPhysIOKB,
        RequestMode,
        SessionID,
        SessionWDID,
        Statements,
        TotalIOCount,
        WarningOnly

    }
}
