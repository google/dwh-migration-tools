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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.databricks.sdk.WorkspaceClient;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.DatabricksFilter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHandleTest {

  private static final int PARALLELISM = 2;
  private static final String CALL_NAME = "tables.get";
  private static final String TIMED_OUT_INPUT = "b";

  @Test
  public void runForEach_skipsObjectWhoseCallTimesOut() throws IOException {
    ImmutableList<String> inputs = ImmutableList.of("a", TIMED_OUT_INPUT, "c");
    List<String> results = new ArrayList<>();
    String warning;
    try (DatabricksHandle handle = newHandle()) {
      warning =
          handle.runForEach(
              CALL_NAME,
              inputs,
              DatabricksHandleTest::callOrTimeOut,
              (input, result) -> results.add(result));
    }

    assertEquals(ImmutableList.of("result of a", "result of c"), results);
    assertEquals(CALL_NAME + " failed for 1 of " + inputs.size() + " objects", warning);
  }

  @Test
  public void runForEach_throwsWhenThreadIsInterrupted() throws IOException {
    try (DatabricksHandle handle = newHandle()) {
      Thread.currentThread().interrupt();
      try {
        assertThrows(
            InterruptedIOException.class,
            () ->
                handle.runForEach(
                    CALL_NAME,
                    ImmutableList.of("a"),
                    DatabricksHandleTest::callOrTimeOut,
                    (input, result) -> {}));
      } finally {
        // Clears the interrupt flag, so that it doesn't leak into other tests.
        Thread.interrupted();
      }
    }
  }

  private static String callOrTimeOut(String input) throws IOException {
    if (TIMED_OUT_INPUT.equals(input)) {
      throw new SocketTimeoutException("Read timed out");
    }

    return "result of " + input;
  }

  private static DatabricksHandle newHandle() {
    return new DatabricksHandle(
        null,
        mock(WorkspaceClient.class),
        new DatabricksFilter(ImmutableList.of(), ImmutableList.of()),
        PARALLELISM);
  }
}
