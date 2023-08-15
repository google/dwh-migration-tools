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
package com.google.edwmigration.dumper.application.dumper.clouddumper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.edwmigration.dumper.application.dumper.MetadataDumper;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class MainTest {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private MetadataRetriever metadataRetriever;
  @Mock private DriverRetriever driverRetriever;

  @Test
  public void run_successSingleConnector() throws Exception {
    MetadataDumper metadataDumper = mock(MetadataDumper.class);
    Main underTest = new Main(() -> metadataDumper, metadataRetriever, driverRetriever);
    when(metadataRetriever.getAttribute("dwh_extractor_configuration"))
        .thenReturn(
            Optional.of(
                "{\"connectors\": [{\"connector\": \"test-db\", \"args\": [\"--port\", \"2222\"]}]}"));

    // Act
    underTest.run();

    // Verify
    ArgumentCaptor<String[]> connectorArgumentsCaptor = ArgumentCaptor.forClass(String[].class);
    verify(metadataDumper).run(connectorArgumentsCaptor.capture());
    assertEquals(
        new String[] {"--connector", "test-db", "--port", "2222"},
        connectorArgumentsCaptor.getValue());
  }

  @Test
  public void run_successMultipleConnectors() throws Exception {
    MetadataDumper metadataDumper1 = mock(MetadataDumper.class);
    MetadataDumper metadataDumper2 = mock(MetadataDumper.class);
    Supplier<MetadataDumper> metadataDumperSupplier = mock(Supplier.class);
    when(metadataDumperSupplier.get()).thenReturn(metadataDumper1, metadataDumper2);
    Main underTest = new Main(metadataDumperSupplier, metadataRetriever, driverRetriever);
    when(metadataRetriever.getAttribute("dwh_extractor_configuration"))
        .thenReturn(
            Optional.of(
                "{\"connectors\": ["
                    + "{\"connector\": \"test-db\", \"args\": [\"--port\", \"2222\"]},"
                    + "{\"connector\": \"test-db-logs\", \"args\": [\"--port\", \"2223\"]}]}"));

    // Act
    underTest.run();

    // Verify
    {
      ArgumentCaptor<String[]> connectorArgumentsCaptor = ArgumentCaptor.forClass(String[].class);
      verify(metadataDumper1).run(connectorArgumentsCaptor.capture());
      assertEquals(
          new String[] {"--connector", "test-db", "--port", "2222"},
          connectorArgumentsCaptor.getValue());
    }
    {
      ArgumentCaptor<String[]> connectorArgumentsCaptor = ArgumentCaptor.forClass(String[].class);
      verify(metadataDumper2).run(connectorArgumentsCaptor.capture());
      assertEquals(
          new String[] {"--connector", "test-db-logs", "--port", "2223"},
          connectorArgumentsCaptor.getValue());
    }
  }

  @Test
  public void run_failsOnMissingConnectorConfiguration() throws Exception {
    MetadataDumper metadataDumper = mock(MetadataDumper.class);
    Main underTest = new Main(() -> metadataDumper, metadataRetriever, driverRetriever);
    when(metadataRetriever.getAttribute("dwh_extractor_configuration"))
        .thenReturn(Optional.of("{}"));

    // Act
    MetadataDumperUsageException e =
        assertThrows(MetadataDumperUsageException.class, () -> underTest.run());

    // Verify
    assertEquals("Extractor configuration must provide at least one connector.", e.getMessage());
  }
}
