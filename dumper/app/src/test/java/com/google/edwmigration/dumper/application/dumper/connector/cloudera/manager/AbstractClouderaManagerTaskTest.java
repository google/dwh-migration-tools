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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AbstractClouderaManagerTaskTest {
  AbstractClouderaManagerTask clouderaManagerTask;

  @Before
  public void setUp() throws Exception {
    clouderaManagerTask =
        new AbstractClouderaManagerTask("") {
          @Override
          protected void doRun(
              TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
              throws Exception {
            throw new UnsupportedOperationException("Test implementation");
          }
        };
  }

  @Test
  public void readJsonTree_validJsonLine_success() throws Exception {
    String jsonLine = "{\"key\": 123}";
    byte[] byteArray = jsonLine.getBytes(StandardCharsets.UTF_8);
    InputStream inStream = new ByteArrayInputStream(byteArray);

    JsonNode jsonObj = clouderaManagerTask.readJsonTree(inStream);

    assertEquals(jsonObj.get("key").asInt(), 123);
  }

  @Test
  public void readJsonTree_validJsonLineWithTrailingSymbols_throwsException() throws Exception {
    String jsonLine = "{\"key\": 123} fff";
    byte[] byteArray = jsonLine.getBytes(StandardCharsets.UTF_8);
    InputStream inStream = new ByteArrayInputStream(byteArray);

    assertThrows(JsonParseException.class, () -> clouderaManagerTask.readJsonTree(inStream));
  }

  @Test
  public void readJsonTree_invalidJsonLine_throwsException() throws Exception {
    String jsonLine = "{\"key\": 123]";
    byte[] byteArray = jsonLine.getBytes(StandardCharsets.UTF_8);
    InputStream inStream = new ByteArrayInputStream(byteArray);

    assertThrows(JsonParseException.class, () -> clouderaManagerTask.readJsonTree(inStream));
  }

  @Test
  public void parseJsonStringToObject_validJsonLine_createObject() throws Exception {
    String jsonLine = "{\"personName\": \"Albus\"}";
    DummyPersonDTO person =
        clouderaManagerTask.parseJsonStringToObject(jsonLine, DummyPersonDTO.class);
    assertEquals(person.getName(), "Albus");
  }

  @Test
  public void parseJsonStringToObject_invalidJsonLine_throwsException() throws Exception {
    String jsonLine = "{\"personName\": \"Albus\"]";
    assertThrows(
        JsonParseException.class,
        () -> clouderaManagerTask.parseJsonStringToObject(jsonLine, DummyPersonDTO.class));
  }

  @Test
  public void parseObjectToJsonLine_createJsonLine() throws Exception {
    DummyPersonDTO albus = new DummyPersonDTO("Albus");
    String jsonLine = clouderaManagerTask.serializeObjectToJsonString(albus);
    assertEquals(jsonLine, "{\"name\":\"Albus\"}");
  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
class DummyPersonDTO {
  private final String name;

  @JsonCreator
  public DummyPersonDTO(@JsonProperty(value = "personName", required = true) String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
