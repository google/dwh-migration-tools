package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.SqlQueryFactory;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataLogsConnector.SharedState;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

public class TeradataLogsJdbcTask extends AbstractJdbcTask<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(TeradataLogsConnector.class);

  @VisibleForTesting public static String EXPRESSION_VALIDITY_QUERY = "SELECT TOP 1 %s FROM %s";

  private final SharedState state;
  private final SqlQueryFactory factory;

  /* pp */ TeradataLogsJdbcTask(
      @Nonnull String targetPath, SharedState state, SqlQueryFactory factory) {
    super(targetPath);
    this.state = Preconditions.checkNotNull(state, "SharedState was null.");
    this.factory = factory;
  }

  @Override
  protected Void doInConnection(
      TaskRunContext context, JdbcHandle jdbcHandle, ByteSink sink, Connection connection)
      throws SQLException {
    String sql = getSql(jdbcHandle);
    ResultSetExtractor<Void> rse = newCsvResultSetExtractor(sink, -1);
    return doSelect(connection, rse, sql);
  }

  @Nonnull
  private String getSql(@Nonnull JdbcHandle handle) {
    BiFunction<String, String, Boolean> validator =
        (expression, table) -> isValid(handle.getJdbcTemplate(), expression, table);
    BiPredicate<String, String> predicate =
        (expression, table) ->
            state.expressionValidity.computeIfAbsent(expression, k -> validator.apply(k, table));
    return factory.getSql(predicate);
  }

  /**
   * Runs a test query to check whether a given projection expression is legal on this Teradata
   * instance.
   */
  @Nonnull
  private Boolean isValid(
      @Nonnull JdbcTemplate template, @Nonnull String expression, @Nonnull String table) {
    String sql = String.format(EXPRESSION_VALIDITY_QUERY, expression, table);
    LOG.info("Checking legality of projection expression '{}' using query: {}", expression, sql);
    try {
      template.query(sql, rs -> {});
      return Boolean.TRUE;
    } catch (DataAccessException e) {
      LOG.info(
          "Attribute '{}' is absent, will use NULL in projection: {}", expression, e.getMessage());
      return Boolean.FALSE;
    }
  }

  @Override
  public String toString() {
    return factory.getSql((c, t) -> true);
  }
}
