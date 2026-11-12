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
package com.google.edwmigration.dumper.application.dumper.connector;

import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_URI;

import com.google.common.base.CharMatcher;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import javax.annotation.Nonnull;

/**
 * Checks the flag values that a connector places into a JDBC URL it builds itself.
 *
 * <p>A value must not contain a character that ends its part of the URL, otherwise the rest of the
 * value is read as another part of the URL. Whitespace and control characters are never accepted. A
 * URL given with --url is used as it is and is not checked here.
 */
public final class JdbcUrlComponents {

  /**
   * Characters that end a host or a name in the URL syntax of at least one driver: the path, query
   * and parameter separators {@code / ? & ,} (MySQL, PostgreSQL, Vertica, Teradata), the property
   * separator and value quotes {@code ; { }} and the instance separator {@code \} (SQL Server), the
   * host list and key-value host syntax {@code , ( ) =} (MySQL, MariaDB, Oracle) and the user info
   * separator {@code @}.
   */
  public static final String DELIMITERS = "/\\?&;,=(){}@";

  /**
   * {@link #DELIMITERS} and {@code #}, which starts the URL fragment in MySQL Connector/J. For host
   * names, which never contain {@code #}, and for names in MySQL and MariaDB URLs.
   */
  public static final String DELIMITERS_WITH_FRAGMENT = DELIMITERS + "#";

  /**
   * {@link #DELIMITERS_WITH_FRAGMENT} without {@code \}, which separates a SQL Server host from an
   * instance name, as in {@code host\instance}.
   */
  public static final String SQL_SERVER_HOST_DELIMITERS =
      CharMatcher.is('\\').removeFrom(DELIMITERS_WITH_FRAGMENT);

  /**
   * {@link #DELIMITERS} and {@code :}, which separates an Oracle SID from the port, and a server
   * type from a service name.
   */
  public static final String ORACLE_NAME_DELIMITERS = DELIMITERS + ":";

  private static final CharMatcher ALWAYS_REJECTED =
      CharMatcher.whitespace().or(CharMatcher.javaIsoControl());

  private static final CharMatcher PRINTABLE_ASCII = CharMatcher.inRange('!', '~');

  private JdbcUrlComponents() {}

  /**
   * Returns the value if it contains none of the given delimiters, no whitespace and no control
   * characters.
   *
   * @param flag the name of the flag the value comes from, without the leading dashes
   * @param value the value placed into the URL
   * @param delimiters the characters that end this part of the URL for the driver
   * @throws MetadataDumperUsageException if the value contains a rejected character
   */
  @Nonnull
  public static String checkUrlComponent(
      @Nonnull String flag, @Nonnull String value, @Nonnull String delimiters) {
    int index = CharMatcher.anyOf(delimiters).or(ALWAYS_REJECTED).indexIn(value);
    if (index < 0) {
      return value;
    }
    throw new MetadataDumperUsageException(
        String.format(
            "The --%s value contains %s, which cannot be used in a JDBC URL built from flags."
                + " Pass the full JDBC URL with --%s instead.",
            flag, describe(value.charAt(index)), OPT_URI));
  }

  @Nonnull
  private static String describe(char c) {
    if (PRINTABLE_ASCII.matches(c)) {
      return "'" + c + "'";
    }
    return String.format("the character U+%04X", (int) c);
  }
}
