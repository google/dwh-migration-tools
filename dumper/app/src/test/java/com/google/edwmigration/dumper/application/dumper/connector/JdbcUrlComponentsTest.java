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

import static com.google.edwmigration.dumper.application.dumper.connector.JdbcUrlComponents.DELIMITERS;
import static com.google.edwmigration.dumper.application.dumper.connector.JdbcUrlComponents.DELIMITERS_WITH_FRAGMENT;
import static com.google.edwmigration.dumper.application.dumper.connector.JdbcUrlComponents.ORACLE_NAME_DELIMITERS;
import static com.google.edwmigration.dumper.application.dumper.connector.JdbcUrlComponents.SQL_SERVER_HOST_DELIMITERS;
import static com.google.edwmigration.dumper.application.dumper.connector.JdbcUrlComponents.checkUrlComponent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class JdbcUrlComponentsTest {

  @DataPoints("hosts")
  public static final ImmutableList<String> HOSTS =
      ImmutableList.of(
          "localhost",
          "db-1.example.com",
          "warehouse_01.corp.example",
          "10.20.30.40",
          "::1",
          "[::1]",
          "[2001:db8::7]",
          "[fe80::1%25eth0]",
          "dbhost:5432");

  @DataPoints("hostDelimiters")
  public static final ImmutableList<String> HOST_DELIMITER_SETS =
      ImmutableList.of(DELIMITERS, DELIMITERS_WITH_FRAGMENT, SQL_SERVER_HOST_DELIMITERS);

  @DataPoints("names")
  public static final ImmutableList<String> NAMES =
      ImmutableList.of(
          "airflow_db", "Sales-2024", "orcl.example.com", "ORCL$1", "SYS#AUX", "Données", "x");

  @DataPoints("nameDelimiters")
  public static final ImmutableList<String> NAME_DELIMITER_SETS =
      ImmutableList.of(DELIMITERS, ORACLE_NAME_DELIMITERS);

  @DataPoints("allDelimiters")
  public static final ImmutableList<String> ALL_DELIMITER_SETS =
      ImmutableList.of(
          DELIMITERS, DELIMITERS_WITH_FRAGMENT, SQL_SERVER_HOST_DELIMITERS, ORACLE_NAME_DELIMITERS);

  @DataPoints("alwaysRejected")
  public static final ImmutableList<Character> ALWAYS_REJECTED =
      ImmutableList.of(
          ' ', '\t', '\n', '\r', '\u000B', '\0', '\u007F', '\u0085', '\u00A0', '\u2028');

  @Theory
  public void checkUrlComponent_host_returnsValue(
      @FromDataPoints("hosts") String host, @FromDataPoints("hostDelimiters") String delimiters) {
    assertEquals(host, checkUrlComponent("host", host, delimiters));
  }

  @Theory
  public void checkUrlComponent_name_returnsValue(
      @FromDataPoints("names") String name, @FromDataPoints("nameDelimiters") String delimiters) {
    assertEquals(name, checkUrlComponent("database", name, delimiters));
  }

  @Theory
  public void checkUrlComponent_delimiter_throwsUsageException(
      @FromDataPoints("allDelimiters") String delimiters) {
    for (char delimiter : delimiters.toCharArray()) {
      String value = "db" + delimiter + "x";

      MetadataDumperUsageException exception =
          assertThrows(
              MetadataDumperUsageException.class,
              () -> checkUrlComponent("database", value, delimiters));

      assertEquals(
          "The --database value contains '"
              + delimiter
              + "', which cannot be used in a JDBC URL built from flags."
              + " Pass the full JDBC URL with --url instead.",
          exception.getMessage());
    }
  }

  @Theory
  public void checkUrlComponent_whitespaceOrControl_throwsUsageException(
      @FromDataPoints("alwaysRejected") Character rejected,
      @FromDataPoints("allDelimiters") String delimiters) {
    String value = "db" + rejected + "x";

    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class, () -> checkUrlComponent("host", value, delimiters));

    assertEquals(
        String.format(
            "The --host value contains the character U+%04X, which cannot be used in a JDBC URL"
                + " built from flags. Pass the full JDBC URL with --url instead.",
            (int) rejected),
        exception.getMessage());
  }

  @Theory
  public void checkUrlComponent_hostWithFragment_throwsUsageException(
      @FromDataPoints("hosts") String host) {
    assertThrows(
        MetadataDumperUsageException.class,
        () -> checkUrlComponent("host", host + "#", DELIMITERS_WITH_FRAGMENT));
  }

  @Test
  public void checkUrlComponent_sqlServerNamedInstance_returnsValue() {
    String host = "dbhost\\SQLEXPRESS";

    assertEquals(host, checkUrlComponent("host", host, SQL_SERVER_HOST_DELIMITERS));
  }

  @Test
  public void checkUrlComponent_oracleNameWithColon_throwsUsageException() {
    assertThrows(
        MetadataDumperUsageException.class,
        () -> checkUrlComponent("oracle-service", "ORCL:POOLED", ORACLE_NAME_DELIMITERS));
  }
}
