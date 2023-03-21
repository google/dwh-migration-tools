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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorExecutionTest;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.ArrayUtils;

/** @author shevek */
public class AbstractSnowflakeConnectorExecutionTest extends AbstractConnectorExecutionTest {

  // TODO("Constants from SnowflakeValidator.")
  private static final ImmutableList<? extends String> ARGS =
      ImmutableList.of(
          "--host", "compilerworks.snowflakecomputing.com",
          "--database", "cw",
          "--warehouse", "cw",
          "--user", "cw",
          "--password", "[redacted]",
          "--role", "dumper");

  @Nonnull
  public static String[] ARGS(
      @Nonnull Connector connector, @Nonnull File outputFile, @Nonnull String... args) {
    List<String> out = new ArrayList<>(4 + ARGS.size() + args.length);

    out.add("--connector");
    out.add(connector.getName());

    out.add("--output");
    out.add(outputFile.getAbsolutePath());

    out.addAll(ARGS);

    Collections.addAll(out, args);

    return out.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
  }
}
