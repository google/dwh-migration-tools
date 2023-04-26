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
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataUtils.formatQuery;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.AbstractTeradataConnector.SharedState;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

public class TeradataUtilityLogsJdbcTask extends AbstractJdbcTask<Summary> {

  private static final DateTimeFormatter SQL_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);

  private static final ImmutableList<String> EXPRESSIONS =
      ImmutableList.of(
          "AcctString",
          "AcctStringDate",
          "AcctStringHour",
          "AcctStringTime",
          "AppID",
          "ClientID",
          "ClientAddr",
          createTimestampExpression("CollectTimeStamp"),
          "CPUDecayLevel",
          "DelayTime",
          "DSAOperation",
          "ExpandAcctString",
          "ExtendedMLoad",
          "FastExportNoSpool",
          "FinalWDID",
          "IODecayLevel",
          createTimestampExpression("JobEndTime"),
          "JobInstanceID",
          "JobName",
          createTimestampExpression("JobStartTime"),
          "LogicalHostID",
          createTimestampExpression("LogonDateTime"),
          "LogonSource",
          "LSN",
          "MaxDataWaitTime",
          "MaxDataWaitTimeID",
          "NumSesOrBuildProc",
          "OpEnvID",
          createTimestampExpression("Phase0EndTime"),
          "Phase0IOKB",
          "Phase0ParserCPUTime",
          "Phase0ParserCPUTimeNorm",
          "Phase0PhysIO",
          "Phase0PhysIOKB",
          createTimestampExpression("Phase0StartTime"),
          "Phase0TotalCPUTime",
          "Phase0TotalCPUTimeNorm",
          "Phase0TotalIO",
          "Phase1BlockCount",
          "Phase1ByteCount",
          createTimestampExpression("Phase1EndTime"),
          "Phase1IOKB",
          "Phase1MaxAMPMemoryUsage",
          "Phase1MaxAWTUsage",
          "Phase1MaxCPUAmpNumber",
          "Phase1MaxCPUAmpNumberNorm",
          "Phase1MaxCPUTime",
          "Phase1MaxCPUTimeNorm",
          "Phase1MaxIO",
          "Phase1MaxIOAmpNumber",
          "Phase1MaxRSGMemoryUsage",
          "Phase1ParserCPUTime",
          "Phase1ParserCPUTimeNorm",
          "Phase1PhysIO",
          "Phase1PhysIOKB",
          "Phase1RowCount",
          "Phase1RSGCPUTime",
          "Phase1RSGCPUTimeNorm",
          createTimestampExpression("Phase1StartTime"),
          "Phase1TotalCPUTime",
          "Phase1TotalCPUTimeNorm",
          "Phase1TotalIO",
          "Phase2BlockCount",
          "Phase2ByteCount",
          createTimestampExpression("Phase2EndTime"),
          "Phase2IOKB",
          "Phase2MaxAMPMemoryUsage",
          "Phase2MaxAWTUsage",
          "Phase2MaxCPUAmpNumber",
          "Phase2MaxCPUAmpNumberNorm",
          "Phase2MaxCPUTime",
          "Phase2MaxCPUTimeNorm",
          "Phase2MaxIO",
          "Phase2MaxIOAmpNumber",
          "Phase2MaxRSGMemoryUsage",
          "Phase2ParserCPUTime",
          "Phase2ParserCPUTimeNorm",
          "Phase2ParserCPUTimeNorm",
          "Phase2PhysIO",
          "Phase2PhysIOKB",
          "Phase2RSGCPUTime",
          "Phase2RSGCPUTimeNorm",
          createTimestampExpression("Phase2StartTime"),
          "Phase2TotalCPUTime",
          "Phase2TotalCPUTimeNorm",
          "Phase2TotalIO",
          createTimestampExpression("Phase3EndTime"),
          "Phase3IOKB",
          "Phase3MaxAMPMemoryUsage",
          "Phase3MaxAWTUsage",
          "Phase3MaxRSGMemoryUsage",
          "Phase3ParserCPUTime",
          "Phase3ParserCPUTimeNorm",
          "Phase3PhysIO",
          "Phase3PhysIOKB",
          "Phase3RSGCPUTime",
          "Phase3RSGCPUTimeNorm",
          createTimestampExpression("Phase3StartTime"),
          "Phase3TotalCPUTime",
          "Phase3TotalCPUTimeNorm",
          "Phase3TotalIO",
          createTimestampExpression("Phase4EndTime"),
          "Phase4IOKB",
          "Phase4ParserCPUTime",
          "Phase4ParserCPUTimeNorm",
          "Phase4PhysIO",
          "Phase4PhysIOKB",
          createTimestampExpression("Phase4StartTime"),
          "Phase4TotalCPUTime",
          "Phase4TotalCPUTimeNorm",
          "Phase4TotalIO",
          "ProcID",
          "ProfileID",
          "ProfileName",
          "ProxyRole",
          "ProxyUser",
          "QueryBand",
          "RowsDeleted",
          "RowsExported",
          "RowsInserted",
          "RowsUpdated",
          "SessionID",
          "SessionWDID",
          "SysConID",
          "TDWMRuleID",
          "UserID",
          "UserName",
          "UtilityName",
          "UtilityRequest",
          "WDID",
          "ZoneID");

  private static final Logger LOG = LoggerFactory.getLogger(TeradataUtilityLogsJdbcTask.class);
  private static final String EXPRESSION_VALIDITY_QUERY = "SELECT TOP 1 %s FROM %s";
  private final SharedState state;
  private final String utilityTable;
  @Nonnull private final ZonedInterval interval;

  protected TeradataUtilityLogsJdbcTask(
      String targetPath, SharedState state, String utilityTable, @Nonnull ZonedInterval interval) {
    super(targetPath);
    this.state = state;
    this.utilityTable = utilityTable;
    this.interval = interval;
  }

  @Override
  protected Summary doInConnection(
      TaskRunContext context, JdbcHandle jdbcHandle, ByteSink sink, Connection connection)
      throws SQLException {
    String sql = getSql(jdbcHandle);
    ResultSetExtractor<Summary> rse = newCsvResultSetExtractor(sink, -1).withInterval(interval);
    return doSelect(connection, rse, sql);
  }

  @Nonnull
  private String getSql(@Nonnull JdbcHandle handle) {
    Function<String, Boolean> validator =
        expression -> isValid(handle.getJdbcTemplate(), expression);
    Predicate<String> predicate =
        expression -> state.expressionValidity.computeIfAbsent(expression, validator);
    return getSql(predicate);
  }

  /**
   * For each potential expression in EXPRESSIONS, work out whether this Teradata accepts it, and if
   * so, use it as part of the eventual query.
   *
   * @param predicate A predicate to compute whether a given expression is legal.
   * @return A SQL query containing every legal expression from EXPRESSIONS.
   */
  private String getSql(@Nonnull Predicate<? super String> predicate) {
    return getSql(predicate, EXPRESSIONS);
  }

  private String getSql(Predicate<? super String> predicate, ImmutableList<String> expressions) {
    StringBuilder buf = new StringBuilder(" SELECT ");
    String separator = "";
    for (String expression : expressions) {
      buf.append(separator);
      if (predicate.test(expression)) {
        buf.append(expression);
      } else {
        buf.append("NULL");
      }
      separator = ", ";
    }
    buf.append(" FROM ").append(utilityTable);
    buf.append(" WHERE CollectTimeStamp >= ");
    appendCastToTimestamp(buf, interval.getStart());
    buf.append(" AND CollectTimeStamp < ");
    appendCastToTimestamp(buf, interval.getEndExclusive());
    return formatQuery(buf.toString());
  }

  private static void appendCastToTimestamp(StringBuilder buf, ZonedDateTime zonedDateTime) {
    buf.append(" CAST('").append(SQL_FORMAT.format(zonedDateTime)).append("' AS TIMESTAMP) ");
  }

  /**
   * Runs a test query to check whether a given projection expression is legal on this Teradata
   * instance.
   */
  private boolean isValid(@Nonnull JdbcTemplate template, @Nonnull String expression) {
    String sql = String.format(EXPRESSION_VALIDITY_QUERY, expression, utilityTable);
    LOG.info("Checking legality of projection expression '{}' using query: {}", expression, sql);
    try {
      template.query(sql, rs -> {});
      return true;
    } catch (DataAccessException e) {
      LOG.info(
          "Projection expression '{}' is not valid, will use NULL in projection: {}",
          expression,
          e.getMessage());
      return false;
    }
  }

  @Override
  public String toString() {
    return getSql(expression -> true);
  }
}
