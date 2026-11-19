/*
 * Copyright 2022-2025 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.handle;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.jdbc.ResultSetExtractor;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is thread-safe as log as the provided {@link DataSource} is thread-safe.
 *
 * @author shevek
 */
public class JdbcHandle implements Handle {

  private static final Logger logger = LoggerFactory.getLogger(JdbcHandle.class);
  static final int DEFAULT_FETCH_SIZE = 1024;
  private static final String STATEMENT_SEPARATOR = ";";
  private static final String LINE_COMMENT_START = "--";
  private static final String BLOCK_COMMENT_START = "/*";
  private static final String BLOCK_COMMENT_END = "*/";

  @Nonnull
  public static JdbcHandle newPooledJdbcHandle(@Nonnull DataSource dataSource, int threadPoolSize)
      throws SQLException {
    HikariConfig config = HandleUtil.createHikariConfig(dataSource, threadPoolSize);
    return new JdbcHandle(new HikariDataSource(config));
  }

  private final DataSource dataSource;

  public JdbcHandle(@Nonnull DataSource dataSource) throws SQLException {
    HandleUtil.verifyJdbcConnection(dataSource);
    this.dataSource = dataSource;
  }

  @Nonnull
  public DataSource getDataSource() {
    return dataSource;
  }

  /**
   * Runs a query that returns one row with one column and returns its value, which can be null. No
   * row, more than one row or more than one column is an error.
   */
  @CheckForNull
  public String queryForString(@Nonnull String sql) throws SQLException {
    return query(
        sql,
        rs -> {
          if (!rs.next()) {
            throw new SQLException("Incorrect result size: expected 1, actual 0");
          }

          int columnCount = rs.getMetaData().getColumnCount();
          if (columnCount != 1) {
            throw new SQLException("Incorrect column count: expected 1, actual " + columnCount);
          }

          String value = rs.getString(1);
          int rowCount = 1;
          while (rs.next()) {
            rowCount++;
          }

          if (rowCount != 1) {
            throw new SQLException("Incorrect result size: expected 1, actual " + rowCount);
          }

          return value;
        });
  }

  @Nonnull
  public List<String> queryStringColumn(@Nonnull String sql, @Nonnull String columnLabel)
      throws SQLException {
    return query(
        sql,
        rs -> {
          List<String> values = new ArrayList<>();
          while (rs.next()) {
            values.add(rs.getString(columnLabel));
          }

          return values;
        });
  }

  /** Runs a query and reads all of its rows, ignoring their content. */
  public void executeQuery(@Nonnull String sql) throws SQLException {
    query(
        sql,
        rs -> {
          while (rs.next()) {
            // Consume all rows.
          }

          return null;
        });
  }

  private <T> T query(String sql, ResultSetExtractor<T> extractor) throws SQLException {
    Connection connection = dataSource.getConnection();
    try {
      Statement statement = connection.createStatement();
      try {
        statement.setFetchSize(DEFAULT_FETCH_SIZE);
        ResultSet rs = statement.executeQuery(sql);
        try {
          return extractor.extractData(rs);
        } finally {
          closeQuietly(rs);
        }
      } finally {
        closeQuietly(statement);
      }
    } finally {
      closeQuietly(connection);
    }
  }

  /**
   * Runs the statements of a script file one by one on a single connection and stops at the first
   * one that fails. The file is read in the platform's default charset and split by {@link
   * #splitSqlStatements(String)}.
   */
  public void executeScript(@Nonnull Path sqlScript) throws IOException, SQLException {
    String script = new String(Files.readAllBytes(sqlScript), Charset.defaultCharset());
    ImmutableList<String> statements = splitSqlStatements(script);
    Connection connection = dataSource.getConnection();
    try {
      Statement statement = connection.createStatement();
      try {
        int statementNumber = 0;
        for (String sql : statements) {
          statementNumber++;
          try {
            statement.execute(sql);
          } catch (SQLException e) {
            // The driver's message alone does not say which statement of the script failed.
            throw new SQLException(
                String.format(
                    "Failed to execute SQL script statement #%d of file [%s]: %s; nested exception is %s",
                    statementNumber, sqlScript.toAbsolutePath(), sql, e),
                e.getSQLState(),
                e.getErrorCode(),
                e);
          }
        }
      } finally {
        closeQuietly(statement);
      }
    } finally {
      closeQuietly(connection);
    }
  }

  // A failure to close must not turn a query that succeeded into a failed one.
  private static void closeQuietly(@Nullable AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception e) {
        logger.trace("Could not close JDBC resource", e);
      }
    }
  }

  /**
   * Splits a SQL script into statements.
   *
   * <ul>
   *   <li>Lines that start with {@code --} are dropped, unless they contain {@code *}{@code /}.
   *   <li>Statements end at {@code ;}. A script that has no {@code ;} is split at line ends.
   *   <li>A separator inside single or double quotes does not end a statement. A backslash keeps
   *       the character after it as it is.
   *   <li>{@code --} comments up to the end of the line and block comments are removed.
   *   <li>Outside quotes, each run of spaces, tabs and line ends becomes one space, and the ones at
   *       the start of a statement are removed.
   * </ul>
   *
   * @throws IllegalArgumentException if the script has only blanks and comment lines, or a block
   *     comment is not closed
   */
  @Nonnull
  public static ImmutableList<String> splitSqlStatements(@Nonnull String script) {
    String text = removeCommentLines(script);
    Preconditions.checkArgument(!StringUtils.isBlank(text), "The SQL script is empty");
    List<String> statements = new ArrayList<>();
    if (!splitAtSeparator(text, STATEMENT_SEPARATOR, statements)) {
      statements.clear();
      splitAtSeparator(text, "\n", statements);
    }

    return ImmutableList.copyOf(statements);
  }

  private static String removeCommentLines(String script) {
    StringBuilder text = new StringBuilder();
    // Splits at \n, \r and \r\n, and does not return an empty last line.
    Iterable<String> lines = new BufferedReader(new StringReader(script)).lines()::iterator;
    for (String line : lines) {
      if (line.startsWith(LINE_COMMENT_START) && !line.contains(BLOCK_COMMENT_END)) {
        continue;
      }

      if (text.length() > 0) {
        text.append('\n');
      }

      text.append(line);
    }

    return text.toString();
  }

  /**
   * Adds the statements of the text to the list and returns whether the text has the separator
   * outside quotes and comments.
   */
  private static boolean splitAtSeparator(String text, String separator, List<String> statements) {
    StringBuilder current = new StringBuilder();
    boolean separatorFound = false;
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
    boolean afterBackslash = false;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (afterBackslash) {
        afterBackslash = false;
        current.append(c);
        continue;
      }

      if (c == '\\') {
        afterBackslash = true;
        current.append(c);
        continue;
      }

      if (c == '\'' && !inDoubleQuote) {
        inSingleQuote = !inSingleQuote;
      } else if (c == '"' && !inSingleQuote) {
        inDoubleQuote = !inDoubleQuote;
      }

      if (!inSingleQuote && !inDoubleQuote) {
        if (text.startsWith(separator, i)) {
          separatorFound = true;
          if (current.length() > 0) {
            statements.add(current.toString());
            current.setLength(0);
          }

          i += separator.length() - 1;
          continue;
        }

        if (text.startsWith(LINE_COMMENT_START, i)) {
          int lineEnd = text.indexOf('\n', i);
          if (lineEnd < 0) {
            break;
          }

          // The line end is skipped with the comment.
          i = lineEnd;
          continue;
        }

        if (text.startsWith(BLOCK_COMMENT_START, i)) {
          int commentEnd = text.indexOf(BLOCK_COMMENT_END, i);
          Preconditions.checkArgument(
              commentEnd >= 0, "The SQL script has a block comment that is not closed");
          i = commentEnd + BLOCK_COMMENT_END.length() - 1;
          continue;
        }

        if (c == ' ' || c == '\r' || c == '\n' || c == '\t') {
          if (current.length() > 0 && current.charAt(current.length() - 1) != ' ') {
            current.append(' ');
          }

          continue;
        }
      }

      current.append(c);
    }

    if (!StringUtils.isBlank(current)) {
      statements.add(current.toString());
    }

    return separatorFound;
  }

  @Override
  public void close() throws IOException {
    DataSource ds = getDataSource();
    if (ds instanceof AutoCloseable) {
      try {
        ((AutoCloseable) ds).close();
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException("Failed to close DataSource: " + e, e);
      }
    }
  }
}
