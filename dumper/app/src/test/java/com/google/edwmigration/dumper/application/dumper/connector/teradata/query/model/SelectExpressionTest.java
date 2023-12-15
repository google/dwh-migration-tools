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
package com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model;

import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.select;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.selectTop;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SelectExpressionTest {

  @Test
  public void select_noProjections_fail() {
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> select(new String[0]).build());

    assertEquals("SELECT requires at least one projection.", e.getMessage());
  }

  @Test
  public void select_topZero_fail() {
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> selectTop(0).build());

    assertEquals("SELECT TOP must use positive integer.", e.getMessage());
  }
}
