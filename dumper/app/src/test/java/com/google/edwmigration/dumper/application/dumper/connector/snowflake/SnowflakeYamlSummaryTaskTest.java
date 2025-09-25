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

import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeYamlSummaryTask.create;
import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat.CompilerWorksDumpMetadataTaskFormat.Root;
import java.io.IOException;
import java.time.Instant;
import java.util.function.LongPredicate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SnowflakeYamlSummaryTaskTest {

  @Test
  public void createRoot_success() throws IOException {
    ImmutableList<String> argumentValues = ImmutableList.of("--connector", "snowflake");
    ConnectorArguments arguments = ConnectorArguments.create(argumentValues);
    LongPredicate timeCheck = millis -> abs(Instant.now().toEpochMilli() - millis) < 10000L;
    SnowflakeYamlSummaryTask task = create("snowflake.metadata.zip", arguments);

    Root root = task.createRoot(null);

    assertEquals("snowflake.metadata.zip", root.format);
    assertTrue(root.product.arguments, root.product.arguments.contains("connector=snowflake"));
    assertTrue(String.valueOf(root.timestamp), timeCheck.test(root.timestamp));
  }
}
