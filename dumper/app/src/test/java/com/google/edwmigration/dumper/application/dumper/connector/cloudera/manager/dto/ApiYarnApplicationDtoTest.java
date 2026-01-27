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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.Test;

public class ApiYarnApplicationDtoTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void applicationIdExists() throws Exception {
    JsonNode jsonNode =
        objectMapper.readTree(readString("/cloudera/manager/dto/ApiYARNApplicationDTO.json"));
    ApiYarnApplicationDto dto = new ApiYarnApplicationDto(jsonNode);

    assertEquals("job_1741847944157_0018", dto.getApplicationId());
  }

  @Test
  public void applicationIdDoesNotExist() throws Exception {
    JsonNode jsonNode = objectMapper.readTree("{ \"prop\": \"val\"}");
    ApiYarnApplicationDto dto = new ApiYarnApplicationDto(jsonNode);

    assertNull("ApplicationId must be null if doesn't exist", dto.getApplicationId());
  }

  @Test
  public void nullJsonFieldHandled() {
    ApiYarnApplicationDto dto = new ApiYarnApplicationDto(null);

    assertNull("ApplicationId must be null for null json", dto.getApplicationId());
  }

  @Test
  public void jsonSerialization() throws Exception {
    JsonNode yarnApp = objectMapper.readTree("{ \"prop\": \"val\"}");
    ApiYarnApplicationDto dto = new ApiYarnApplicationDto(yarnApp);

    JsonNode outputJsonl = objectMapper.convertValue(dto, JsonNode.class);

    // json filed apiYarnApplication is expected on server side
    assertEquals(yarnApp, outputJsonl.at("/apiYarnApplication"));
  }

  private String readString(String name) throws IOException, URISyntaxException {
    return new String(Files.readAllBytes(Paths.get(this.getClass().getResource(name).toURI())));
  }
}
