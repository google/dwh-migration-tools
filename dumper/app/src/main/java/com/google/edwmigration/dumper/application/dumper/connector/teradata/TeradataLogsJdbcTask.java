/*
 * Copyright 2022-2024 Google LLC
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

import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataLogsConnector.DBQLSQLTBL_SQLTEXTINFO_LENGTH;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataUtils.formatQuery;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.cast;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.eq;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.identifier;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.stringLiteral;
import static com.google.edwmigration.dumper.application.dumper.utils.OptionalUtils.optionallyWhen;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSink;
import com.google.common.primitives.Ints;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.AbstractTeradataConnector.SharedState;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Expression;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.utils.QueryLogDateUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

public class TeradataLogsJdbcTask extends AbstractJdbcTask<Summary> {
  private static final Logger LOG = LoggerFactory.getLogger(TeradataLogsConnector.class);

  protected static final DateTimeFormatter SQL_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);
  protected static final DateTimeFormatter SQL_DATE_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE.withZone(ZoneOffset.UTC);
  // Docref: https://docs.teradata.com/reader/wada1XMYPkZVTqPKz2CNaw/F7f64mU9~e4s03UAdorEHw
  // According to one customer, the attributes "SQLTextInfo", "LastRespTime",
  // "RequestMode", and "Statements" are all absent from DBQLogTbl in version 14.10.07.10,
  // and possibly others, hence our need to replace any missing projection attributes with NULLs.
  // MUST match TeradataLogsDumpFormat.Header
  @VisibleForTesting
  static final ImmutableList<String> EXPRESSIONS =
      ImmutableList.of(
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
          "L.StartTime");

  @VisibleForTesting /* pp */ static String EXPRESSION_VALIDITY_QUERY = "SELECT TOP 1 %s FROM %s";
  protected final SharedState state;
  protected final QueryLogTableNames tableNames;
  protected final ImmutableSet<String> conditions;
  protected final ZonedInterval interval;
  @CheckForNull private final String logDateColumn;
  private final OptionalLong maxSqlLength;
  protected final ImmutableList<String> orderBy;
  private final ImmutableList<String> expressions;

  private Optional<String> sql = Optional.empty();

  public TeradataLogsJdbcTask(
      @Nonnull String targetPath,
      SharedState state,
      QueryLogTableNames queryLogTableNames,
      Set<String> conditions,
      ZonedInterval interval) {
    this(
        targetPath,
        state,
        queryLogTableNames,
        conditions,
        interval,
        /* logDateColumn= */ null,
        /* maxSqlLength= */ OptionalLong.empty(),
        /* orderBy= */ ImmutableList.of(),
        EXPRESSIONS);
  }

  protected TeradataLogsJdbcTask(
      @Nonnull String targetPath,
      SharedState state,
      QueryLogTableNames tableNames,
      Set<String> conditions,
      ZonedInterval interval,
      @CheckForNull String logDateColumn,
      OptionalLong maxSqlLength,
      List<String> orderBy,
      List<String> expressions) {
    super(targetPath);
    this.state = Preconditions.checkNotNull(state, "SharedState was null.");
    this.tableNames = tableNames;
    this.conditions = ImmutableSet.copyOf(conditions);
    this.interval = interval;
    this.logDateColumn = logDateColumn;
    this.maxSqlLength = maxSqlLength;
    this.orderBy = ImmutableList.copyOf(orderBy);
    this.expressions = ImmutableList.copyOf(expressions);
  }

  private static boolean isQueryTable(@Nonnull String expression) {
    return expression.startsWith("ST.");
  }

  @Override
  protected Summary doInConnection(
      TaskRunContext context, JdbcHandle jdbcHandle, ByteSink sink, Connection connection)
      throws SQLException {
    String sql = getOrCreateSql(jdbcHandle);
    ResultSetExtractor<Summary> rse = newCsvResultSetExtractor(sink);
    Summary summary = doSelect(connection, withInterval(rse, interval), sql);
    if (summary.rowCount() > 0) {
      QueryLogDateUtil.updateQueryLogDates(interval.getStart(), interval.getEndExclusive());
    }
    return summary;
  }

  @Nonnull
  private String getOrCreateSql(@Nonnull JdbcHandle handle) {
    Function<String, Boolean> validator =
        expression -> isValid(handle.getJdbcTemplate(), expression);
    Predicate<String> predicate =
        expression -> state.expressionValidity.computeIfAbsent(expression, validator);
    return getOrCreateSql(predicate, expressions);
  }

  /**
   * For each potential expression in EXPRESSIONS, work out whether this Teradata accepts it, and if
   * so, use it as part of the eventual query.
   *
   * @param predicate A predicate to compute whether a given expression is legal.
   * @return A SQL query containing every legal expression from EXPRESSIONS.
   */
  @Nonnull
  /* pp */ String getOrCreateSql(Predicate<String> predicate, List<String> localExpressions) {
    if (!sql.isPresent()) {
      sql = Optional.of(buildSql(predicate, localExpressions));
    }
    return sql.get();
  }

  private String buildSql(Predicate<String> predicate, List<String> localExpressions) {
    StringBuilder buf = new StringBuilder("SELECT ");
    String separator = "";
    boolean queryTableIncluded = false;
    for (String expression : localExpressions) {
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

    buf.append(" FROM ").append(tableNames.queryLogsTableName()).append(" L");

    if (queryTableIncluded) {
      // "QueryID is a system-wide unique field; you can use QueryID
      // to join DBQL tables ... without needing ProcID as an additional join field."
      // (https://docs.teradata.com/reader/B7Lgdw6r3719WUyiCSJcgw/YIKoBz~QQgv2Aw5dF339kA)
      buf.append(" LEFT OUTER JOIN ");
      if (maxSqlLength.isPresent()) {
        buf.append('(')
            .append(
                createSubQueryWithSplittingLongQueries(Ints.checkedCast(maxSqlLength.getAsLong())))
            .append(')');
      } else {
        buf.append(tableNames.sqlLogsTableName());
      }
      buf.append(" ST ON (L.QueryID=ST.QueryID");

      if (logDateColumn != null) {
        buf.append(" AND L.").append(logDateColumn).append("=ST.").append(logDateColumn);
      }
      buf.append(')');

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

    if (logDateColumn != null) {
      buf.append(" AND L.").append(createLogDateColumnConditionStr());
    }

    for (String condition : conditions) {
      buf.append(" AND ").append(condition);
    }

    if (!orderBy.isEmpty()) {
      buf.append(" ORDER BY ");
      Joiner.on(", ").appendTo(buf, orderBy);
    }
    return formatQuery(buf.toString());
  }

  private String createLogDateColumnConditionStr() {
    return logDateColumn + " = CAST('" + SQL_DATE_FORMAT.format(interval.getStart()) + "' AS DATE)";
  }

  private Expression createLogDateColumnCondition() {
    return eq(
        identifier(logDateColumn),
        cast(stringLiteral(SQL_DATE_FORMAT.format(interval.getStart())), identifier("DATE")));
  }

  private String createSubQueryWithSplittingLongQueries(int maxLength) {
    ImmutableList.Builder<String> columns = ImmutableList.<String>builder().add("QueryID");
    if (logDateColumn != null) {
      columns.add(logDateColumn);
    }
    return new SplitTextColumnQueryGenerator(
            columns.build(),
            "SqlTextInfo",
            "SqlRowNo",
            tableNames.sqlLogsTableName(),
            /* whereCondition=*/ optionallyWhen(
                logDateColumn != null, this::createLogDateColumnCondition),
            DBQLSQLTBL_SQLTEXTINFO_LENGTH,
            maxLength)
        .generate();
  }

  /**
   * Runs a test query to check whether a given projection expression is legal on this Teradata
   * instance.
   */
  @Nonnull
  private Boolean isValid(@Nonnull JdbcTemplate template, @Nonnull String expression) {
    String table =
        isQueryTable(expression)
            ? tableNames.sqlLogsTableName() + " ST"
            : tableNames.queryLogsTableName() + " L";
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
  public String describeSourceData() {
    return sql.map(AbstractTask::createSourceDataDescriptionForQuery)
        .orElseGet(
            () ->
                String.format(
                    "from tables '%s' and '%s'",
                    tableNames.queryLogsTableName(), tableNames.sqlLogsTableName()));
  }
}
