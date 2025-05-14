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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.time.Clock;
import java.util.List;
import javax.annotation.Nonnull;
import org.junit.Test;

public class ConnectorTest {
  private final Connector connector = new EmptyConnector();

  @Test
  public void validateDateRange_startDateAndEndDate_success() throws Exception {
    String argsStr = "--connector test --start-date=2001-02-20 --end-date=2001-02-25";

    // Act
    connector.validateDateRange(toArgs(argsStr));
  }

  @Test
  public void validateDateRange_startDateAfterEndDate_throws() {
    String argsStr = "--connector test --start-date=2001-02-20 --end-date=2001-02-20";

    Exception exception =
        assertThrows(
            IllegalStateException.class, () -> connector.validateDateRange(toArgs(argsStr)));
    assertEquals(
        "Start date [2001-02-20T00:00Z] must be before end date [2001-02-20T00:00Z].",
        exception.getMessage());
  }

  @Test
  public void validateDateRange_endDateAlone_throws() {
    String argsStr = "--connector test --end-date=2001-02-20";

    Exception exception =
        assertThrows(
            IllegalStateException.class, () -> connector.validateDateRange(toArgs(argsStr)));
    assertEquals(
        "End date can be specified only with start date, but start date was null.",
        exception.getMessage());
  }

  @Test
  public void validateDateRange_startDateAlone_throws() {
    String argsStr = "--connector test --start-date=2001-02-20";

    Exception exception =
        assertThrows(RuntimeException.class, () -> connector.validateDateRange(toArgs(argsStr)));
    assertEquals(
        "End date must be specified with start date, but was null.", exception.getMessage());
  }

  @Test
  public void validateDateRange_requiredArgs_success() throws Exception {
    // Act
    connector.validateDateRange(toArgs("--connector test"));
  }

  private static class EmptyConnector implements Connector {

    @Nonnull
    @Override
    public String getName() {
      return null;
    }

    @Nonnull
    @Override
    public String getDefaultFileName(boolean isAssessment, Clock clock) {
      return null;
    }

    @Override
    public void addTasksTo(
        @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
        throws Exception {}

    @Nonnull
    @Override
    public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
      return null;
    }

    @Nonnull
    @Override
    public Iterable<ConnectorProperty> getPropertyConstants() {
      return null;
    }
  }

  private static ConnectorArguments toArgs(String args) throws Exception {
    return new ConnectorArguments(args.split(" "));
  }
}
