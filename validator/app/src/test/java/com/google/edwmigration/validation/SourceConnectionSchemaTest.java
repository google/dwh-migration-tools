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
package com.google.edwmigration.validation;

import static org.junit.Assert.*;

import com.google.edwmigration.validation.config.SourceConnection;
import com.google.edwmigration.validation.deformed.Deformed;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author pbreeden */
@RunWith(JUnit4.class)
public class SourceConnectionSchemaTest {

  private static final Deformed<SourceConnection> VALIDATOR =
      new Deformed<>(SourceConnection.buildSchema());

  @Test
  public void passes_with_valid_fields_and_values() {
    SourceConnection sc = new SourceConnection();
    sc.connectionType = "JDBC";
    sc.database = "my_db";
    sc.driver = "postgres";
    sc.host = "localhost";
    sc.password = "secret";
    sc.port = "5432";
    sc.user = "admin";

    assertTrue(VALIDATOR.validate(sc));
  }

  @Test
  public void fails_with_missing_fields() {
    SourceConnection sc = new SourceConnection(); // all fields null
    assertFalse(VALIDATOR.validate(sc));
  }

  @Test
  public void fails_with_non_number_port() {
    SourceConnection sc = new SourceConnection();
    sc.connectionType = "JDBC";
    sc.database = "my_db";
    sc.driver = "postgres";
    sc.host = "localhost";
    sc.password = "secret";
    sc.port = "not_a_number";
    sc.user = "admin";

    assertFalse(VALIDATOR.validate(sc));
  }

  @Test
  public void fails_if_port_is_out_of_range() {
    SourceConnection sc = new SourceConnection();
    sc.connectionType = "JDBC";
    sc.database = "my_db";
    sc.driver = "postgres";
    sc.host = "localhost";
    sc.password = "secret";
    sc.port = "70000"; // invalid
    sc.user = "admin";

    assertFalse(VALIDATOR.validate(sc));
  }

  @Test
  public void passes_with_port_value_in_range() {
    SourceConnection scMin = new SourceConnection();
    scMin.connectionType = "JDBC";
    scMin.database = "db";
    scMin.driver = "pg";
    scMin.host = "host";
    scMin.password = "pw";
    scMin.port = "1";
    scMin.user = "user";

    assertTrue(VALIDATOR.validate(scMin));

    SourceConnection scMax = new SourceConnection();
    scMax.connectionType = "JDBC";
    scMax.database = "db";
    scMax.driver = "pg";
    scMax.host = "host";
    scMax.password = "pw";
    scMax.port = "65535";
    scMax.user = "user";

    assertTrue(VALIDATOR.validate(scMax));
  }
}
