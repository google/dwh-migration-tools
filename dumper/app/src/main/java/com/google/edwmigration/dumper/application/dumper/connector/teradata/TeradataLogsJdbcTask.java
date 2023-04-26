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

import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataUtils.formatQuery;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.AbstractTeradataConnector.SharedState;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.errorprone.annotations.ForOverride;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

public class TeradataLogsJdbcTask extends AbstractJdbcTask<Summary> {

  protected static final DateTimeFormatter SQL_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);
  // Docref: https://docs.teradata.com/reader/wada1XMYPkZVTqPKz2CNaw/F7f64mU9~e4s03UAdorEHw
  // According to one customer, the attributes "SQLTextInfo", "LastRespTime",
  // "RequestMode", and "Statements" are all absent from DBQLogTbl in version 14.10.07.10,
  // and possibly others, hence our need to replace any missing projection attributes with NULLs.
  // MUST match TeradataLogsDumpFormat.Header
  @VisibleForTesting
  static final String[] EXPRESSIONS =
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

  private static final Logger LOG = LoggerFactory.getLogger(TeradataLogsConnector.class);
  @VisibleForTesting /* pp */ static String EXPRESSION_VALIDITY_QUERY = "SELECT TOP 1 %s FROM %s";
  protected final SharedState state;
  protected final String logTable;
  protected final String queryTable;
  protected final List<String> conditions;
  protected final ZonedInterval interval;
  protected final List<String> orderBy;

  public TeradataLogsJdbcTask(
      @Nonnull String targetPath,
      SharedState state,
      String logTable,
      String queryTable,
      List<String> conditions,
      ZonedInterval interval) {
    this(targetPath, state, logTable, queryTable, conditions, interval, Collections.emptyList());
  }

  protected TeradataLogsJdbcTask(
      @Nonnull String targetPath,
      SharedState state,
      String logTable,
      String queryTable,
      List<String> conditions,
      ZonedInterval interval,
      List<String> orderBy) {
    super(targetPath);
    this.state = Preconditions.checkNotNull(state, "SharedState was null.");
    this.logTable = logTable;
    this.queryTable = queryTable;
    this.conditions = conditions;
    this.interval = interval;
    this.orderBy = orderBy;
  }

  private static boolean isQueryTable(@Nonnull String expression) {
    return expression.startsWith("ST.");
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
    String sql = getSql(predicate);
    // LOG.debug("SQL is " + sql);
    return sql;
  }

  /**
   * For each potential expression in EXPRESSIONS, work out whether this Teradata accepts it, and if
   * so, use it as part of the eventual query.
   *
   * @param predicate A predicate to compute whether a given expression is legal.
   * @return A SQL query containing every legal expression from EXPRESSIONS.
   */
  @ForOverride
  @Nonnull
  /* pp */ String getSql(@Nonnull Predicate<? super String> predicate) {
    return getSql(predicate, EXPRESSIONS);
  }

  /* pp */ String getSql(Predicate<? super String> predicate, String[] expressions) {
    StringBuilder buf = new StringBuilder("SELECT ");
    String separator = "";
    boolean queryTableIncluded = false;
    for (String expression : expressions) {
      buf.append(separator);
      if (predicate.test(expression)) {
        buf.append(expression);
        if (isQueryTable(expression)) {
          queryTableIncluded = true;
        }
      } else {
        buf.append("NULL");
      }
      separator = ", ";
    }

    buf.append(" FROM ").append(logTable).append(" L");

    if (queryTableIncluded) {
      // "QueryID is a system-wide unique field; you can use QueryID
      // to join DBQL tables ... without needing ProcID as an additional join field."
      // (https://docs.teradata.com/reader/B7Lgdw6r3719WUyiCSJcgw/YIKoBz~QQgv2Aw5dF339kA)
      buf.append(" LEFT OUTER JOIN ").append(queryTable).append(" ST ON L.QueryID=ST.QueryID");

      // Notwithstanding the above: could this offer improved perf due to use of indices?:
      // http://elsasoft.com/samples/teradata/Teradata.127.0.0.1.DBC/table_DBQLSqlTbl.htm
      // http://elsasoft.com/samples/teradata/Teradata.127.0.0.1.DBC/table_DBQLogTbl.htm
      // Testing on a PostgreSQL-backed-db indicates this is actually slightly slower; maybe on-site
      // actual TD will perform better?
      // buf.append(" LEFT OUTER JOIN ").append(queryTable).append(" ST ON L.ProcID=ST.ProcID AND
      // L.CollectTimeStamp=ST.CollectTimeStamp AND L.QueryID=ST.QueryID");
    }

    buf.append(
        String.format(
            " WHERE L.ErrorCode=0\n"
                + "AND L.StartTime >= CAST('%s' AS TIMESTAMP)\n"
                + "AND L.StartTime < CAST('%s' AS TIMESTAMP)\n",
            SQL_FORMAT.format(interval.getStart()), SQL_FORMAT.format(interval.getEndExclusive())));

    for (String condition : conditions) {
      buf.append(" AND ").append(condition);
    }

    if (!orderBy.isEmpty()) {
      buf.append(" ORDER BY ");
      Joiner.on(", ").appendTo(buf, orderBy);
    }
    return buf.toString().replace('\n', ' ');
  }

  /**
   * Runs a test query to check whether a given projection expression is legal on this Teradata
   * instance.
   */
  @Nonnull
  private Boolean isValid(@Nonnull JdbcTemplate template, @Nonnull String expression) {
    String table = isQueryTable(expression) ? queryTable + " ST" : logTable + " L";
    String sql = formatQuery(String.format(EXPRESSION_VALIDITY_QUERY, expression, table));
    LOG.info("Checking legality of projection expression '{}' using query: {}", expression, sql);
    try {
      template.query(sql, rs -> {});
      return Boolean.TRUE;
    } catch (DataAccessException e) {
      LOG.info(
          "Projection expression '{}' is not valid, will use NULL in projection: {}",
          expression,
          e.getMessage());
      return Boolean.FALSE;
    }
  }

  @Override
  public String toString() {
    return getSql(Predicates.alwaysTrue());
  }
}
