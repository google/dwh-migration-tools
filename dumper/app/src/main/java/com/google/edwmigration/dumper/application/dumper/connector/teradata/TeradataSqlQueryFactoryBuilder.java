package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import com.google.common.annotations.VisibleForTesting;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.SqlQueryFactory;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataLogsDumpFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TeradataSqlQueryFactoryBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(TeradataSqlQueryFactoryBuilder.class);
  @VisibleForTesting
  /* pp */ static final String DEF_LOG_TABLE = "dbc.DBQLogTbl";
  private static final String DEF_QUERY_TABLE = "dbc.DBQLSQLTbl";
  private static final String ASSESSMENT_DEF_LOG_TABLE = "dbc.QryLogV";

  @VisibleForTesting
  /* pp */ static final List<String> EXPRESSIONS_LSQL_TBL = enumNames("ST.", TeradataLogsDumpFormat.HeaderLSql.class);
  @VisibleForTesting
  /* pp */ static final List<String> EXPRESSIONS_LOG_TBL = enumNames("L.", TeradataLogsDumpFormat.HeaderLog.class);
  @VisibleForTesting
  /* pp */ static final String[] EXPRESSIONS =
      new String[] {
        "L.QueryID",
        "ST.SQLRowNo",
        "ST.SQLTextInfo",
        "L.UserName",
        "L.CollectTimeStamp",
        "L.StatementType",
        "L.AppID",
        "L.DefaultDatabase",
        "L.ErrorCode",
        "L.ErrorText",
        "L.FirstRespTime",
        "L.LastRespTime",
        "L.NumResultRows",
        "L.QueryText",
        "L.ReqPhysIO",
        "L.ReqPhysIOKB",
        "L.RequestMode",
        "L.SessionID",
        "L.SessionWDID",
        "L.Statements",
        "L.TotalIOCount",
        "L.WarningOnly",
        "L.StartTime"
      };

  private static final String[] EXPRESSIONS_FOR_ASSESSMENT =
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
        "L.CollectTimeStamp AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS \"CollectTimeStamp\"",
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
        "L.FirstRespTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS \"FirstRespTime\"",
        "L.FirstStepTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS \"FirstStepTime\"",
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
        "L.LogonDateTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS \"LogonDateTime\"",
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
        "L.StartTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS \"StartTime\"",
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

  private ZonedInterval interval;
  private String logTable;
  private String queryTable;
  private List<String> conditions;
  private List<String> orderColumns;
  private List<String> expressions;
  private boolean isAssessment;
  // For Terdata version <= 14
  private boolean isOldVersion;
  private final boolean isLSql;

  /* pp */ static TeradataSqlQueryFactoryBuilder startBuildingFrom(
      ConnectorArguments arguments, boolean forLSql) throws MetadataDumperUsageException {
    TeradataSqlQueryFactoryBuilder builder = new TeradataSqlQueryFactoryBuilder(forLSql);
    builder.parse(arguments);
    return builder;
  }

  /* pp */ static TeradataSqlQueryFactoryBuilder startBuildingFrom(ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    return startBuildingFrom(arguments, false);
  }

  /* pp */ TeradataSqlQueryFactoryBuilder within(ZonedInterval interval) {
    this.interval = interval;
    return this;
  }

  private TeradataSqlQueryFactoryBuilder(boolean isLSql) {
    this.isLSql = isLSql;
  }

  private void parse(ConnectorArguments arguments) throws MetadataDumperUsageException {
    isAssessment = arguments.isAssessment();
    isOldVersion = arguments.getConnectorName().startsWith("teradata14");
    parseAlternates(arguments.getQueryLogAlternates());
    addConditions(arguments.getQueryLogEarliestTimestamp());
    addOrderColumns();
    selectExpressions();
  }

  private void parseAlternates(List<String> alternates) throws MetadataDumperUsageException {
    logTable = isAssessment ? ASSESSMENT_DEF_LOG_TABLE : DEF_LOG_TABLE;
    queryTable = DEF_QUERY_TABLE;
    if (alternates != null && !alternates.isEmpty()) {
      if (alternates.size() != 2)
        throw new MetadataDumperUsageException(
            "Alternate query log tables must be given as a pair; you specified: " + alternates);
      logTable = alternates.get(0);
      queryTable = alternates.get(1);
    }
  }

  private void addConditions(String earliestTimestamp) {
    conditions = new ArrayList<>();
    // if the user specifies an earliest start time there will be extraneous empty dump files
    // because we always iterate over the full 7 trailing days; maybe it's worth
    // preventing that in the future. To do that, we should require getQueryLogEarliestTimestamp()
    // to parse and return an ISO instant, not a database-server-specific format.
    if (!StringUtils.isBlank(earliestTimestamp)) {
      if (isLSql) {
        conditions.add("ST.CollectTimeStamp >= " + earliestTimestamp);
      } else {
        conditions.add("L.StartTime >= " + earliestTimestamp);
      }
    }
    if (!(isOldVersion || isAssessment)) conditions.add("L.UserName <> 'DBC'");
  }

  private void addOrderColumns() {
    orderColumns =
        isOldVersion || !isAssessment
            ? Collections.emptyList()
            : Arrays.asList("ST.QueryID", "ST.SQLRowNo");
  }

  private void selectExpressions() {
    if (isLSql) expressions = EXPRESSIONS_LSQL_TBL;
    else if (isOldVersion) expressions = EXPRESSIONS_LOG_TBL;
    else {
      String[] selections = isAssessment ? EXPRESSIONS_FOR_ASSESSMENT : EXPRESSIONS;
      expressions = Arrays.asList(selections);
    }
  }

  private static List<String> enumNames(String prefix, Class<? extends Enum<?>> en) {
    Enum<?> v[] = en.getEnumConstants();
    List<String> ret = new ArrayList<>(v.length);
    for (Enum<?> h : v) ret.add(prefix + h.name());
    return ret;
  }

  /* pp */ SqlQueryFactory build() {
    return new TeradataSqlQueryFactory(
        interval,
        logTable,
        queryTable,
        isOldVersion ? "" : "L.ErrorCode=0 AND",
        isLSql ? queryTable + " ST " : logTable + " L ",
        isLSql ? "ST.CollectTimeStamp" : "L.StartTime",
        conditions,
        orderColumns,
        expressions,
        !isOldVersion);
  }
}
