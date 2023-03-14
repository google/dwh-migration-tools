package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static com.google.edwmigration.dumper.application.dumper.connector.teradata.AbstractTeradataConnector.SQL_FORMAT;

import com.google.common.base.Joiner;
import com.google.edwmigration.dumper.application.dumper.connector.SqlQueryFactory;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class TeradataSqlQueryFactory implements SqlQueryFactory {
  private final ZonedInterval interval;
  private final String logTable;
  private final String queryTable;
  private final String errorCheckStatement;
  private final String baseTableName;
  private final String timeFieldName;
  private final List<String> conditions;
  private final List<String> orderColumns;
  private final List<String> expressions;
  private final boolean joinQueryTable;

  /* pp */ TeradataSqlQueryFactory(
      ZonedInterval interval,
      String logTable,
      String queryTable,
      String errorCheckStatement,
      String baseTableName,
      String timeFieldName,
      List<String> conditions,
      List<String> orderColumns,
      List<String> expressions,
      boolean joinQueryTable) {
    this.interval = interval;
    this.logTable = logTable;
    this.queryTable = queryTable;
    this.errorCheckStatement = errorCheckStatement;
    this.baseTableName = baseTableName;
    this.timeFieldName = timeFieldName;
    this.conditions = conditions;
    this.orderColumns = orderColumns;
    this.expressions = expressions;
    this.joinQueryTable = joinQueryTable;
  }

  private String getTableFrom(String columnName) {
    return isQueryTable(columnName) ? queryTable + " ST" : logTable + " L";
  }

  /**
   * For each potential expression in EXPRESSIONS, work out whether this Teradata accepts it, and if
   * so, use it as part of the eventual query.
   *
   * @param predicate A predicate to compute whether a given expression is legal.
   * @return A SQL query containing every legal expression from EXPRESSIONS.
   */
  @Override
  public String getSql(BiPredicate<String, String> predicate) {
    return getBaseExpressions(predicate)
        .append(createJoiningStatement())
        .append(parseConditions())
        .append(parseOrderColumns())
        .toString()
        .trim()
        .replace('\n', ' ')
        .replaceAll(" {2,}", " ");
  }

  private StringBuilder parseOrderColumns() {
    StringBuilder builder = new StringBuilder();
    if (!orderColumns.isEmpty()) {
      builder.append(" ORDER BY ");
      Joiner.on(", ").appendTo(builder, orderColumns);
    }
    return builder;
  }

  private StringBuilder parseConditions() {
    StringBuilder builder = new StringBuilder();
    builder.append(
        String.format(
            " WHERE %1$s %2$s >= CAST('%3$s' AS TIMESTAMP)\n"
                + "AND %2$s < CAST('%4$s' AS TIMESTAMP)\n",
            errorCheckStatement,
            timeFieldName,
            SQL_FORMAT.format(interval.getStart()),
            SQL_FORMAT.format(interval.getEndExclusive())));
    for (String condition : conditions) {
      builder.append(" AND ").append(condition);
    }
    return builder;
  }

  private boolean isQueryTable(@Nonnull String expression) {
    return expression.startsWith("ST.");
  }

  private StringBuilder createJoiningStatement() {
    StringBuilder builder = new StringBuilder();
    if (joinQueryTable && expressions.stream().anyMatch(this::isQueryTable)) {
      // "QueryID is a system-wide unique field; you can use QueryID
      // to join DBQL tables ... without needing ProcID as an additional join field."
      // (https://docs.teradata.com/reader/B7Lgdw6r3719WUyiCSJcgw/YIKoBz~QQgv2Aw5dF339kA)
      builder.append(" LEFT OUTER JOIN ").append(queryTable).append(" ST ON L.QueryID=ST.QueryID");

      // Notwithstanding the above: could this offer improved perf due to use of indices?:
      // http://elsasoft.com/samples/teradata/Teradata.127.0.0.1.DBC/table_DBQLSqlTbl.htm
      // http://elsasoft.com/samples/teradata/Teradata.127.0.0.1.DBC/table_DBQLogTbl.htm
      // Testing on a PostgreSQL-backed-db indicates this is actually slightly slower; maybe
      // on-site
      // actual TD will perform better?
      // buf.append(" LEFT OUTER JOIN ").append(queryTable).append(" ST ON L.ProcID=ST.ProcID
      // AND
      // L.CollectTimeStamp=ST.CollectTimeStamp AND L.QueryID=ST.QueryID");
    }
    return builder;
  }

  private StringBuilder getBaseExpressions(BiPredicate<String, String> predicate) {
    Function<String, String> transformToValid =
        expression -> predicate.test(expression, getTableFrom(expression)) ? expression : "NULL";
    String columnNames =
        expressions.stream().map(transformToValid).collect(Collectors.joining(", "));
    return new StringBuilder("SELECT ").append(columnNames).append(" FROM ").append(baseTableName);
  }
}
