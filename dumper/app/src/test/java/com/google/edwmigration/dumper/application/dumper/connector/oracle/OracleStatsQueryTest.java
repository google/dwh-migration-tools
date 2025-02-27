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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import static com.google.edwmigration.dumper.application.dumper.connector.oracle.QueryGroup.TenantSetup.MULTI_TENANT;
import static java.time.Duration.ofDays;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OracleStatsQueryTest {

  @Test
  public void description_success() {
    boolean isRequired = true;
    OracleStatsQuery query =
        OracleStatsQuery.createNative("db-objects", isRequired, ofDays(30), MULTI_TENANT);
    assertEquals("Query{name=db-objects, statsSource=NATIVE}", query.description());
  }
}
