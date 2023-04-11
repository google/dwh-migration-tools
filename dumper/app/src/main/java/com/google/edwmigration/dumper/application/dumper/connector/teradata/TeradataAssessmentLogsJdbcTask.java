/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataUtils.createTimestampExpression;

import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.AbstractTeradataConnector.SharedState;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nonnull;

public class TeradataAssessmentLogsJdbcTask extends TeradataLogsJdbcTask {

  private static final String QUERY_LOG_TABLE_ALIAS = "L";
  static final String[] EXPRESSIONS =
      new String[] {
        "L.QueryID",
        "ST.SQLRowNo",
        "ST.SQLTextInfo",
        "L.AbortFlag",
        "L.AcctString",
        "L.AcctStringDate",
        "L.AcctStringHour",
        "L.AcctStringTime",
        "L.AMPCPUTime",
        "L.AMPCPUTimeNorm",
        "L.AppID",
        "L.CacheFlag",
        "L.CalendarName",
        "L.CallNestingLevel",
        "L.CheckpointNum",
        "L.ClientAddr",
        "L.ClientID",
        createTimestampExpression(QUERY_LOG_TABLE_ALIAS, "CollectTimeStamp"),
        "L.CPUDecayLevel",
        "L.DataCollectAlg",
        "L.DBQLStatus",
        "L.DefaultDatabase",
        "L.DelayTime",
        "L.DisCPUTime",
        "L.DisCPUTimeNorm",
        "L.ErrorCode",
        "L.EstMaxRowCount",
        "L.EstMaxStepTime",
        "L.EstProcTime",
        "L.EstResultRows",
        "L.ExpandAcctString",
        createTimestampExpression(QUERY_LOG_TABLE_ALIAS, "FirstRespTime"),
        createTimestampExpression(QUERY_LOG_TABLE_ALIAS, "FirstStepTime"),
        "L.FlexThrottle",
        "L.ImpactSpool",
        "L.InternalRequestNum",
        "L.IODecayLevel",
        "L.IterationCount",
        "L.KeepFlag",
        "L.LastRespTime",
        "L.LockDelay",
        "L.LockLevel",
        "L.LogicalHostID",
        createTimestampExpression(QUERY_LOG_TABLE_ALIAS, "LogonDateTime"),
        "L.LogonSource",
        "L.LSN",
        "L.MaxAMPCPUTime",
        "L.MaxAMPCPUTimeNorm",
        "L.MaxAmpIO",
        "L.MaxCPUAmpNumber",
        "L.MaxCPUAmpNumberNorm",
        "L.MaxIOAmpNumber",
        "L.MaxNumMapAMPs",
        "L.MaxOneMBRowSize",
        "L.MaxStepMemory",
        "L.MaxStepsInPar",
        "L.MinAmpCPUTime",
        "L.MinAmpCPUTimeNorm",
        "L.MinAmpIO",
        "L.MinNumMapAMPs",
        "L.MinRespHoldTime",
        "L.NumFragments",
        "L.NumOfActiveAMPs",
        "L.NumRequestCtx",
        "L.NumResultOneMBRows",
        "L.NumResultRows",
        "L.NumSteps",
        "L.NumStepswPar",
        "L.ParamQuery",
        "L.ParserCPUTime",
        "L.ParserCPUTimeNorm",
        "L.ParserExpReq",
        "L.PersistentSpool",
        "L.ProcID",
        "L.ProfileID",
        "L.ProfileName",
        "L.ProxyRole",
        "L.ProxyUser",
        "L.ProxyUserID",
        "L.QueryBand",
        "L.QueryRedriven",
        "L.QueryText",
        "L.ReDriveKind",
        "L.RemoteQuery",
        "L.ReqIOKB",
        "L.ReqPhysIO",
        "L.ReqPhysIOKB",
        "L.RequestMode",
        "L.RequestNum",
        "L.SeqRespTime",
        "L.SessionID",
        "L.SessionTemporalQualifier",
        "L.SpoolUsage",
        createTimestampExpression(QUERY_LOG_TABLE_ALIAS, "StartTime"),
        "L.StatementGroup",
        "L.Statements",
        "L.StatementType",
        "L.SysDefNumMapAMPs",
        "L.TacticalCPUException",
        "L.TacticalIOException",
        "L.TDWMEstMemUsage",
        "L.ThrottleBypassed",
        "L.TotalFirstRespTime",
        "L.TotalIOCount",
        "L.TotalServerByteCount",
        "L.TTGranularity",
        "L.TxnMode",
        "L.TxnUniq",
        "L.UnitySQL",
        "L.UnityTime",
        "L.UsedIota",
        "L.UserID",
        "L.UserName",
        "L.UtilityByteCount",
        "L.UtilityInfoAvailable",
        "L.UtilityRowCount",
        "L.VHLogicalIO",
        "L.VHLogicalIOKB",
        "L.VHPhysIO",
        "L.VHPhysIOKB",
        "L.WarningOnly",
        "L.WDName"
      };

  public TeradataAssessmentLogsJdbcTask(
      @Nonnull String targetPath,
      SharedState state,
      String logTable,
      String queryTable,
      List<String> conditions,
      ZonedInterval interval,
      List<String> orderBy) {
    super(targetPath, state, logTable, queryTable, conditions, interval, orderBy);
  }

  @Nonnull
  @Override
  String getSql(@Nonnull Predicate<? super String> predicate) {
    return getSql(predicate, EXPRESSIONS);
  }
}
