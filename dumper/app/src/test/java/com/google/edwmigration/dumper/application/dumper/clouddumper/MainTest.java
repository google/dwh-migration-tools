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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumper;
import java.util.Optional;
import org.junit.Before;
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

  @Mock private MetadataDumper metadataDumper;
  @Mock private MetadataRetriever metadataRetriever;

  private Main underTest;

  @Before
  public void setUp() {
    underTest = new Main(metadataDumper, metadataRetriever);
  }

  @Test
  public void run_success() throws Exception {
    when(metadataRetriever.getAttribute("dwh_connector")).thenReturn(Optional.of("test-db"));
    when(metadataRetriever.getAttribute("dwh_extractor_configuration"))
        .thenReturn(Optional.of("{\"args\": [\"--port\", \"2222\"]}"));

    // Act
    underTest.run();

    // Verify
    ArgumentCaptor<ConnectorArguments> connectorArgumentsCaptor =
        ArgumentCaptor.forClass(ConnectorArguments.class);
    verify(metadataDumper).run(connectorArgumentsCaptor.capture());
    ConnectorArguments connectorArguments = connectorArgumentsCaptor.getValue();
    assertEquals(Integer.valueOf(2222), connectorArguments.getPort());
    assertEquals("test-db", connectorArguments.getConnectorName());
  }
}
