package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static org.junit.Assert.assertEquals;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.SqlQueryFactory;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import org.junit.Test;

public class TeradataSqlQueryFactoryBuilderTest {

  @Test
  public void test_buildLSqlFactory() throws IOException, MetadataDumperUsageException {
    //  Arrange
    ConnectorArguments arguments =
        new ConnectorArguments(new String[] {"--connector", "teradata14"});
    ZonedDateTime endDate = ZonedDateTime.parse("2023-03-09T00:00:00Z");
    ZonedDateTime startDate = endDate.minusDays(5).truncatedTo(ChronoUnit.DAYS);
    ZonedInterval interval = new ZonedInterval(startDate, endDate);
    String expectedQuery =
        "SELECT "
            + "ST.QueryID, ST.CollectTimeStamp, ST.SQLRowNo, ST.SQLTextInfo "
            + "FROM dbc.DBQLSQLTbl ST "
            + "WHERE ST.CollectTimeStamp >= CAST('2023-03-04T00:00:00Z' AS TIMESTAMP) "
            + "AND ST.CollectTimeStamp < CAST('2023-03-09T00:00:00Z' AS TIMESTAMP)";

    //  Act
    SqlQueryFactory factory =
        TeradataSqlQueryFactoryBuilder.startBuildingFrom(arguments, true).within(interval).build();
    String generatedQuery = factory.getSql((c, t) -> true);

    //  Assert
    assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void test_buildOldLogFactory() throws IOException, MetadataDumperUsageException {
    //  Arrange
    ConnectorArguments arguments =
        new ConnectorArguments(new String[] {"--connector", "teradata14"});
    ZonedDateTime endDate = ZonedDateTime.parse("2023-03-09T00:00:00Z");
    ZonedDateTime startDate = endDate.minusDays(5).truncatedTo(ChronoUnit.DAYS);
    ZonedInterval interval = new ZonedInterval(startDate, endDate);
    String expectedQuery =
        "SELECT "
            + "L.CollectTimeStamp, L.UserName, L.StatementType, L.AppID, L.DefaultDatabase, "
            + "L.ErrorCode, L.ErrorText, L.FirstRespTime, L.LastRespTime, L.NumResultRows, "
            + "L.QueryText, L.ReqPhysIO, L.ReqPhysIOKB, L.RequestMode, L.SessionID, L.SessionWDID, "
            + "L.Statements, L.TotalIOCount, L.WarningOnly, L.StartTime "
            + "FROM dbc.DBQLogTbl L "
            + "WHERE L.StartTime >= CAST('2023-03-04T00:00:00Z' AS TIMESTAMP) "
            + "AND L.StartTime < CAST('2023-03-09T00:00:00Z' AS TIMESTAMP)";

    //  Act
    SqlQueryFactory factory =
        TeradataSqlQueryFactoryBuilder.startBuildingFrom(arguments).within(interval).build();
    String generatedQuery = factory.getSql((c, t) -> true);

    //  Assert
    assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void test_buildStandardFactory() throws IOException, MetadataDumperUsageException {
    //  Arrange
    ConnectorArguments arguments = new ConnectorArguments(new String[] {"--connector", "teradata"});
    ZonedDateTime endDate = ZonedDateTime.parse("2023-03-09T00:00:00Z");
    ZonedDateTime startDate = endDate.minusDays(5).truncatedTo(ChronoUnit.DAYS);
    ZonedInterval interval = new ZonedInterval(startDate, endDate);
    String expectedQuery =
        "SELECT "
            + "L.QueryID, ST.SQLRowNo, ST.SQLTextInfo, L.UserName, L.CollectTimeStamp, "
            + "L.StatementType, L.AppID, L.DefaultDatabase, L.ErrorCode, L.ErrorText, "
            + "L.FirstRespTime, L.LastRespTime, L.NumResultRows, L.QueryText, L.ReqPhysIO, "
            + "L.ReqPhysIOKB, L.RequestMode, L.SessionID, L.SessionWDID, L.Statements, "
            + "L.TotalIOCount, L.WarningOnly, L.StartTime "
            + "FROM dbc.DBQLogTbl L "
            + "LEFT OUTER JOIN dbc.DBQLSQLTbl ST ON L.QueryID=ST.QueryID "
            + "WHERE L.ErrorCode=0 AND L.StartTime >= CAST('2023-03-04T00:00:00Z' AS TIMESTAMP) "
            + "AND L.StartTime < CAST('2023-03-09T00:00:00Z' AS TIMESTAMP) "
            + "AND L.UserName <> 'DBC'";

    //  Act
    SqlQueryFactory factory =
        TeradataSqlQueryFactoryBuilder.startBuildingFrom(arguments).within(interval).build();
    String generatedQuery = factory.getSql((c, t) -> true);

    //  Assert
    assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void test_buildAssessmentFactory() throws IOException, MetadataDumperUsageException {
    //  Arrange
    ConnectorArguments arguments =
        new ConnectorArguments(new String[] {"--connector", "teradata", "--assessment"});
    ZonedDateTime endDate = ZonedDateTime.parse("2023-03-09T00:00:00Z");
    ZonedDateTime startDate = endDate.minusDays(5).truncatedTo(ChronoUnit.DAYS);
    ZonedInterval interval = new ZonedInterval(startDate, endDate);
    String expectedQuery =
        "SELECT L.QueryID, ST.SQLRowNo, ST.SQLTextInfo, L.AbortFlag, L.AcctString,"
            + " L.AcctStringDate, L.AcctStringHour, L.AcctStringTime, L.AMPCPUTime,"
            + " L.AMPCPUTimeNorm, L.AppID, L.CacheFlag, L.CalendarName, L.CallNestingLevel,"
            + " L.CheckpointNum, L.ClientAddr, L.ClientID, L.CollectTimeStamp AT TIME ZONE INTERVAL"
            + " '0:00' HOUR TO MINUTE AS \"CollectTimeStamp\", L.CPUDecayLevel, L.DataCollectAlg,"
            + " L.DBQLStatus, L.DefaultDatabase, L.DelayTime, L.DisCPUTime, L.DisCPUTimeNorm,"
            + " L.ErrorCode, L.EstMaxRowCount, L.EstMaxStepTime, L.EstProcTime, L.EstResultRows,"
            + " L.ExpandAcctString, L.FirstRespTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS"
            + " \"FirstRespTime\", L.FirstStepTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS"
            + " \"FirstStepTime\", L.FlexThrottle, L.ImpactSpool, L.InternalRequestNum,"
            + " L.IODecayLevel, L.IterationCount, L.KeepFlag, L.LastRespTime, L.LockDelay,"
            + " L.LockLevel, L.LogicalHostID, L.LogonDateTime AT TIME ZONE INTERVAL '0:00' HOUR TO"
            + " MINUTE AS \"LogonDateTime\", L.LogonSource, L.LSN, L.MaxAMPCPUTime,"
            + " L.MaxAMPCPUTimeNorm, L.MaxAmpIO, L.MaxCPUAmpNumber, L.MaxCPUAmpNumberNorm,"
            + " L.MaxIOAmpNumber, L.MaxNumMapAMPs, L.MaxOneMBRowSize, L.MaxStepMemory,"
            + " L.MaxStepsInPar, L.MinAmpCPUTime, L.MinAmpCPUTimeNorm, L.MinAmpIO, L.MinNumMapAMPs,"
            + " L.MinRespHoldTime, L.NumFragments, L.NumOfActiveAMPs, L.NumRequestCtx,"
            + " L.NumResultOneMBRows, L.NumResultRows, L.NumSteps, L.NumStepswPar, L.ParamQuery,"
            + " L.ParserCPUTime, L.ParserCPUTimeNorm, L.ParserExpReq, L.PersistentSpool, L.ProcID,"
            + " L.ProfileID, L.ProfileName, L.ProxyRole, L.ProxyUser, L.ProxyUserID, L.QueryBand,"
            + " L.QueryRedriven, L.QueryText, L.ReDriveKind, L.RemoteQuery, L.ReqIOKB, L.ReqPhysIO,"
            + " L.ReqPhysIOKB, L.RequestMode, L.RequestNum, L.SeqRespTime, L.SessionID,"
            + " L.SessionTemporalQualifier, L.SpoolUsage, L.StartTime AT TIME ZONE INTERVAL '0:00'"
            + " HOUR TO MINUTE AS \"StartTime\", L.StatementGroup, L.Statements, L.StatementType,"
            + " L.SysDefNumMapAMPs, L.TacticalCPUException, L.TacticalIOException,"
            + " L.TDWMEstMemUsage, L.ThrottleBypassed, L.TotalFirstRespTime, L.TotalIOCount,"
            + " L.TotalServerByteCount, L.TTGranularity, L.TxnMode, L.TxnUniq, L.UnitySQL,"
            + " L.UnityTime, L.UsedIota, L.UserID, L.UserName, L.UtilityByteCount,"
            + " L.UtilityInfoAvailable, L.UtilityRowCount, L.VHLogicalIO, L.VHLogicalIOKB,"
            + " L.VHPhysIO, L.VHPhysIOKB, L.WarningOnly, L.WDName FROM dbc.QryLogV L LEFT OUTER"
            + " JOIN dbc.DBQLSQLTbl ST ON L.QueryID=ST.QueryID WHERE L.ErrorCode=0 AND L.StartTime"
            + " >= CAST('2023-03-04T00:00:00Z' AS TIMESTAMP) AND L.StartTime <"
            + " CAST('2023-03-09T00:00:00Z' AS TIMESTAMP) ORDER BY ST.QueryID, ST.SQLRowNo";

    //  Act
    SqlQueryFactory factory =
        TeradataSqlQueryFactoryBuilder.startBuildingFrom(arguments).within(interval).build();
    String generatedQuery = factory.getSql((c, t) -> true);

    //  Assert
    assertEquals(expectedQuery, generatedQuery);
  }
}
