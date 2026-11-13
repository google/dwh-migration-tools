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

import static com.google.edwmigration.dumper.application.dumper.connector.Connector.validateDateRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.InputDescriptor;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriver;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataMetadataConnector;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class ConnectorTest {

  @Test
  public void getAcceptsInputs_inputsFromComposedAnnotations_included() {
    List<String> keys = new ArrayList<>();
    for (InputDescriptor descriptor : Connector.getAcceptsInputs(new TeradataMetadataConnector())) {
      keys.add(descriptor.getKey());
    }

    // @RespectsArgumentDatabasePredicate is on the connector class, @RespectsArgumentUser on its
    // superclass, and the port is a plain @RespectsInput on the superclass.
    ImmutableList<String> expected =
        ImmutableList.of(
            "--" + ConnectorArguments.OPT_DATABASE,
            "--" + ConnectorArguments.OPT_USER,
            "--" + ConnectorArguments.OPT_PORT);
    assertTrue("Inputs: " + keys, keys.containsAll(expected));
  }

  @Test
  public void printHelp_inputsFromComposedAnnotations_listed() throws Exception {
    StringBuilder help = new StringBuilder();

    new TeradataMetadataConnector().printHelp(help);

    assertTrue(help.toString(), help.toString().contains(RespectsArgumentDriver.DESCRIPTION));
  }

  @Test
  public void validateDateRange_startDateAndEndDate_success() throws Exception {
    ConnectorArguments args =
        new ConnectorArguments(
            "--connector", "test", "--start-date=2001-02-20", "--end-date=2001-02-25");

    // Act
    validateDateRange(args);
  }

  @Test
  public void validateDateRange_startDateAfterEndDate_throws() throws Exception {
    ConnectorArguments args =
        new ConnectorArguments(
            "--connector", "test", "--start-date=2001-02-20", "--end-date=2001-02-20");

    Exception exception =
        assertThrows(RuntimeException.class, () -> Connector.validateDateRange(args));
    assertEquals(
        "Start date [2001-02-20T00:00Z] must be before end date [2001-02-20T00:00Z].",
        exception.getMessage());
  }

  @Test
  public void validateDateRange_endDateAlone_throws() throws Exception {
    ConnectorArguments args =
        new ConnectorArguments("--connector", "test", "--end-date=2001-02-20");

    Exception exception = assertThrows(RuntimeException.class, () -> validateDateRange(args));
    assertEquals(
        "End date can be specified only with start date, but start date was null.",
        exception.getMessage());
  }

  @Test
  public void validateDateRange_startDateAlone_throws() throws Exception {
    ConnectorArguments args =
        new ConnectorArguments("--connector", "test", "--start-date=2001-02-20");

    Exception exception = assertThrows(RuntimeException.class, () -> validateDateRange(args));
    assertEquals(
        "End date must be specified with start date, but was null.", exception.getMessage());
  }

  @Test
  public void validateDateRange_requiredArgs_success() throws Exception {
    // Act
    validateDateRange(new ConnectorArguments("--connector", "test"));
  }
}
