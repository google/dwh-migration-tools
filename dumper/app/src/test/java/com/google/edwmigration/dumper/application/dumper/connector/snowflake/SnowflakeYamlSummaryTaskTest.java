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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeYamlSummaryTask.createRoot;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeYamlSummaryTask.rootString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.task.MemoryByteSink;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SnowflakeYamlSummaryTaskTest {

  @Test
  public void rootString_success() {

    String rootString = rootString(true, Optional.of("2345"));

    assertTrue(rootString, rootString.contains("assessment"));
    assertTrue(rootString, rootString.contains("true"));
    assertTrue(rootString, rootString.contains("warehouse_count"));
    assertTrue(rootString, rootString.contains("2345"));
  }

  @Test
  public void createRoot_countAbsent_setToZero() {

    ImmutableMap<String, ?> root = createRoot(true, Optional.empty());

    assertTrue(root.toString(), root.containsKey("metadata"));
    ImmutableMap<?, ?> value = (ImmutableMap<?, ?>) root.get("metadata");
    assertEquals(0, value.get("warehouse_count"));
  }

  @Test
  public void createRoot_noAssessment_success() {

    ImmutableMap<String, ?> root = createRoot(false, Optional.of("123"));

    assertTrue(root.toString(), root.containsKey("metadata"));
    ImmutableMap<?, ?> value = (ImmutableMap<?, ?>) root.get("metadata");
    assertEquals(false, value.get("assessment"));
    assertEquals(123, value.get("warehouse_count"));
  }

  @Test
  public void extractor_doExtract_writesValidYamlMap() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(1)).thenReturn("2345");
    MemoryByteSink sink = new MemoryByteSink();

    new SnowflakeYamlSummaryTask.Extractor(sink, true).doExtract(resultSet);

    String written = new String(sink.toByteArray(), UTF_8);
    assertFalse(
        "YAML should not be double-serialized as a block scalar: " + written,
        written.startsWith("|"));
    @SuppressWarnings("unchecked")
    Map<String, Object> parsed = new ObjectMapper(new YAMLFactory()).readValue(written, Map.class);
    @SuppressWarnings("unchecked")
    Map<String, Object> metadata = (Map<String, Object>) parsed.get("metadata");
    assertEquals(true, metadata.get("assessment"));
    assertEquals(2345, metadata.get("warehouse_count"));
  }
}
