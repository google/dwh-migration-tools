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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop;

import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.junit.BeforeClass;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class HadoopMetadataConnectorTest {

  @BeforeClass
  public static void setUp() {
    ScriptTmpDirCleanup.cleanupAfterAllTestsAreFinished();
  }

  @DataPoints("scriptNames")
  public static final ImmutableList<String> SCRIPT_NAMES = HadoopMetadataConnector.SCRIPT_NAMES;

  @Theory
  public void readScript_startsWithShebang_success(@FromDataPoints("scriptNames") String scriptName)
      throws IOException {
    String scriptBody = new String(HadoopScripts.read(scriptName + ".sh"));
    assertTrue(scriptBody.startsWith("#!/bin/bash"));
  }
}
